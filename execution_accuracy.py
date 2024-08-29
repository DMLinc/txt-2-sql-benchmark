import itertools
import pandas as pd
import streamlit as st
from compare import compare_results
import numpy as np
from sqlalchemy import create_engine

from typing import Tuple
from tqdm import tqdm

from concurrent.futures import ThreadPoolExecutor



SNOWFLAKE_CONNECTION_TEMPLATE = 'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse=PROD_KLABS_DWH'



def try_run_query(sql: str, con) -> pd.DataFrame | str:
    try:
        return pd.read_sql_query(sql, con=con)
    except Exception as e:
        print(f'failed to execute: {e}')
        return e

# stolen with minimal modifications from https://github.com/defog-ai/sql-eval/blob/313725e8df7f0ebda86560509248e27f106e9dee/eval/eval.py#L104-L156
def find_bracket_indices(s: str, start_index: int = 0) -> "tuple[int, int]":
    start = s.find("{", start_index)
    end = s.find("}", start + 1)
    if start == -1 or end == -1:
        return (-1, -1)
    return (start, end)

def get_all_minimal_queries(query: str) -> list[str]:
    """
    extrapolate all possible queries
    - split by semicolon. this is to accommodate queries where joins to other tables are also acceptable.
    - expand all column permutations if there are braces { } in it. eg:
    ```sql
        SELECT {user.id, user.name} FROM user;
    ```
    Would be expanded to:
    ```sql
        SELECT user.id FROM user;
        SELECT user.name FROM user;
        SELECT user.id, user.name FROM user;
    ```
    """
    queries = query.split(";")
    result_queries = []
    for query in queries:
        query = query.strip()
        if query == "":
            continue
        start, end = find_bracket_indices(query, 0)
        if (start, end) == (-1, -1):
            result_queries.append(query)
            continue
        else:
            # get all possible column subsets
            column_options = query[start + 1 : end].split("|")
            column_combinations = list(
                itertools.chain.from_iterable(
                    itertools.combinations(column_options, r)
                    for r in range(1, len(column_options) + 1)
                )
            )
            for column_tuple in column_combinations:
                left = query[:start]
                column_str = ", ".join(column_tuple)
                right = query[end + 1 :]
                # change group by size dynamically if necessary
                if right.find("GROUP BY {}"):
                    right = right.replace("GROUP BY {}", f"GROUP BY {column_str}")
                result_queries.append(left + column_str + right)
    return result_queries

def sort_func(score):
    if score is np.nan:
        return -1
    else:
        return score + 0

def compare_queries(benchmark_sql: str, submission_sql: str, con) -> dict[str, bool|str]:
    benchmark_queries = get_all_minimal_queries(benchmark_sql)
    benchmark_df_variants = [try_run_query(q, con) for q in benchmark_queries]
    benchmark_variants_isdf = [isinstance(df, pd.DataFrame) for df in benchmark_df_variants]
    any_benchmark_executed = any(benchmark_variants_isdf)
    #benchmark_loc_df = try_run_query(benchmark_sql, con)
    submission_loc_df = try_run_query(submission_sql, con)
    # print(submission_loc_df)
    # print(isinstance(submission_loc_df, pd.DataFrame))
    if any_benchmark_executed and isinstance(submission_loc_df, pd.DataFrame):
        variants_results = []
        for variant_df in [var for i, var in enumerate(benchmark_df_variants) if benchmark_variants_isdf[i]]:
            try:
                res = compare_results(
                    df_true=variant_df,
                    df_pred=submission_loc_df
                )
                if res:
                    variants_results.append({'score': res, 'reason': 'No error'})
                else:
                    variants_results.append({'score': res, 'reason': 'Incorrect output'})
            except ValueError as e:
                variants_results.append({'score': np.nan, 'reason': f'Failed to compare results: {e}'})
        
        best_result = sorted(variants_results, key=lambda d: sort_func(d['score']), reverse=True)[0]
        return best_result

    else:
        bench_err = "" if any_benchmark_executed else f"\ncouldn't execute any benchmark variant. Example: {benchmark_df_variants[0]}"
        subm_err = "" if isinstance(submission_loc_df, pd.DataFrame) else f"\nsubmission query: {submission_loc_df}"
        return {'score': False, 'reason': f"Failed to execute:"+bench_err+subm_err}

def execution_accuracy(benchmark: pd.DataFrame, submission_df: pd.DataFrame, creds: str):
    merged = pd.merge(
        benchmark,
        submission_df,
        on=['question', 'schema'],
        how='left',
        suffixes=['_correct', '_submitted']
    )

    coupon_con = create_engine(SNOWFLAKE_CONNECTION_TEMPLATE.format(**creds, schema='coupon_platform')).connect()
    ecom_con = create_engine(SNOWFLAKE_CONNECTION_TEMPLATE.format(**creds, schema='ecom')).connect()

    con_dict = {'COUPON_PLATFORM': coupon_con, 'ECOM': ecom_con}
    payloads = [(row['sql_correct'], row['sql_submitted'], con_dict[row['schema']]) for i, row in merged.iterrows()]
    
    func = lambda x: compare_queries(x[0], x[1], x[2])
    #futures = [executor.submit(func, doc) for doc in payloads]

    with ThreadPoolExecutor(max_workers=20) as executor:
        print(len(payloads))
        futures = [executor.submit(func, doc) for doc in payloads]
        results = [future.result() for future in futures]
        #print(f"Results: {results}")

    # for i, row in tqdm(merged.iterrows()):
    #     # print(row['correct_sql_query'])
    #     # print(row['submitted_sql'])
    #     score_dict = compare_queries(row['sql_correct'], row['sql_submitted'], row['schema'], creds)
    #     score_dict['question'] = row['question']
    #     results.append(score_dict)
    #print(results)
    #print( pd.DataFrame(results))
    #return pd.merge(merged, pd.DataFrame(results), on='question')
    return pd.concat((merged, pd.DataFrame(results)), axis=1)

if __name__ == '__main__':
    creds = dict(
        account="HU82657.us-central1.gcp",
        user="KSENII2",
        password="zTUJdgsdxypRErSV7z5Y",
        database="sql_eval",
    )
    
    con_templ = 'snowflake://{user}:{password}@{account}/{database}/{schema}'

    
    fake_benchmark_df = pd.read_csv(r'C:\Users\computer\My Drive (nik@atad.ml)\llm-tests\benchmark_app\data\real_benchmark_v2.csv')
    fake_sub_df = pd.read_csv(r'C:\Users\computer\My Drive (nik@atad.ml)\llm-tests\benchmark_app\data\real_submission_gpt4.csv')
    print('fake_bench', fake_benchmark_df.shape)
    print('fake_sub', fake_sub_df.shape)
    curr_time = pd.Timestamp.now()
    print(execution_accuracy(fake_benchmark_df, fake_sub_df, creds))
    time_spend = pd.Timestamp.now() - curr_time
    print(time_spend)
