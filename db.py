import json
import pandas as pd
import streamlit as st


con = st.connection("snowflake")

# result = cursor.execute("SELECT GET_DDL('TABLE', 'coupon_platform.events');").fetchall()
# for r in result:
#     print(r[0])


df = pd.read_csv('samples/sample3.csv')

for _, r in df.iterrows():
    print(r['question'])
    pd.read_sql(r['sql'], con).to_csv(f'samples/answers/{r["question_id"]}.csv', index=False)
    print(pd.read_sql(r['sql'], con))#.to_csv(f'samples/answers/{r["question_id"]}.csv', index=False)
    break
