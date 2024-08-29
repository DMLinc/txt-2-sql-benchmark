import uuid
import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector

from datetime import datetime


from sqlalchemy import create_engine
from static.header_text import header_text
from static.description import description
from static.method_details import methodology
from compare import compare_results
from execution_accuracy import execution_accuracy


account, user, password, database = st.secrets.snowflake_admin_user.values()
print(user, password, account, database)
print(f'snowflake://{user}:{password}@{account}/{database}/public')
admin_engine = create_engine(f'snowflake://{user}:{password}@{account}/{database}/public')

benchmark_df = pd.read_csv('data/real_benchmark_v2.csv')


@st.cache_data 
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

st.set_page_config(
    page_title="Text-2-SQL Benchmark",
    page_icon="static/icon.jpg",
    layout="wide"
)
st.image("static/background-header1.png")
st.title("Text-2-SQL Benchmark")
# Create two columns for the layout
left_column, center, right_column = st.columns([50, 10, 40])


right_column.markdown("## Want to submit your own predictions?\nFollow these steps:\n\n1.Download the archive with sample submission file and schema definitions:")
with open('data/atadml_sql_benchmark_submission_files.zip', 'rb') as f:
    right_column.download_button('Download Submission Files', f, mime='application/zip', file_name="atadml_sql_benchmark_submission_files.zip")
right_column.markdown("2. Generate queries for each question & fill the column `sql` with it")
right_column.markdown('3. Fill the form below & click "submit predictions"')
# left_column.markdown('Don't want to waste time at generating predictions? Submit the sample file for the test')

with right_column.form("Submission form") as form:
    # Create text input fields for name and email
    name = st.text_input("Your name", )
    email = st.text_input("Your email")
    llm_name = st.text_input("The name of your LLM/submission")
    file = st.file_uploader('upload csv predictions', type='csv')
    
    # Form submission button
    submitted = st.form_submit_button("Submit predictions")
    
    # This block simulates sending data to a database on submission
    if submitted:
        if name and email and llm_name and file:
            st.session_state['state'] = 'processing'
            with st.spinner('Waiting for your submission to evaluate... Should take around 1 min'):
               submission_df = pd.read_csv(file)
               submission_time =  datetime.utcnow()
               submission_id = uuid.uuid4().hex
   
               results = execution_accuracy(benchmark_df, submission_df, st.secrets.snowflake_readonly_user)
               eascore = round(results['score'].mean() * 100, 8)
               results['submission_id'] = submission_id
               results = results[['submission_id'] + results.columns.tolist()[:-1]]
               
               sub = pd.Series(
                   (submission_id, name, email, llm_name, datetime.utcnow(), eascore),
                   index=('submission_id', 'name', 'email', 'llm_name', 'submitted_at', 'score'))
   
               with admin_engine.connect() as con:
                   
                   results.to_sql('results', con, if_exists='append', index=False, )
                   sub.to_frame().T.to_sql('submissions', con, if_exists='append', index=False)
            st.success(f'Success! Your score is {eascore}. See you on the leaderboard!')


            #right_column.write(f"The execution accuracy score is {eascore}")
            #right_column.download_button('Download results', convert_df(results), mime='text/csv', file_name="results.csv", key='download-csv')
            #right_column.table(results)
        else:
            st.write(":red[Please fill all the fields]", )


# Use the left column to display text
#file = left_column.file_uploader('upload csv predictions', type='csv')
right_column.markdown(description)
left_column.markdown(header_text)

left_column.markdown("## Results chart")
left_column.bar_chart(
    data=pd.read_sql(
        "select concat(LLM_NAME, ' - ', NAME) as submission ,CAST(score AS INTEGER) AS score from submissions order by score asc;",
        admin_engine.connect()
    ),
    x='submission',
    y='score',
)

left_column.markdown("## Leaderboard")
left_column.table(
    pd.read_sql(
        'select rank() over (order by score desc) as "rank", llm_name as "LLM name", name as "submitted by", CAST(score AS INTEGER) as "score, %" from submissions order by score desc',
        admin_engine.connect()
    ).set_index('rank'),

)
left_column.markdown(methodology)

# Use the right column to display a table
# right_column.write("This is the right panel, displaying a table.")
#right_column.table(df)
#right_column.table(pd.read_sql('select * from coupon_platform.AFFILIATE_NETWORKS limit 10' , con_viewer))


# if file is not None:
#     st.session_state['state'] = 'processing'
#     submission_df = pd.read_csv(file)
#     results = execution_accuracy(benchmark_df, submission_df, con_readonly)
#     eascore = results['score'].mean()
#     right_column.write(f"The execution accuracy score is {eascore}")
#     right_column.table(results)


# def read_file(file) -> pd.DataFrame:
#     try:
#         df = pd.read_csv(file)
#     except Exception as e:
#         st.error(e)
#         return None
#     if not set(cols).issubset(set(df.columns)):
#         return None
#     return df

# st.text('csv format: question_id,schema_name,sql,question')
# file = st.file_uploader('upload csv predictions', type='csv')

# df = read_file(file)
# print(df)
# if df is not None:
#     # st.session_state['state'] = 'processing'
#     for _, row in df.iterrows():
#         res = conn.query(row['sql'])
#         st.text(row['question'])
#         st.dataframe(res)
#         try:
#             answer = pd.read_csv(f'samples/answers/{row["question_id"]}.csv')
#             st.dataframe(answer)
#         except:
#             print('error opening the file')
#             answer = None
#         if answer is not None:
#             correct = compare_results(answer, res)
#         else:
#             correct = False, 'wrong question_id'
#         st.text(f'is correct: {correct}')
