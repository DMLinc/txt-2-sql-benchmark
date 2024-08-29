methodology = """
## Methodology:
This SQL Benchmark consists of ~200 Question/Query samples run against a database which tracks the use and creation of coupons across events, newsletters and stores. The SQL queries were designed to test window functions, CTEs, pre-aggregations, REGEX, multiple joins, NULL value handling and JSON-formatting, among others. We use our own custom version of Execution Accuracy, in which we allow for extra number of columns, differing column names and differing row and column order. Just for reference, the Execution Accuracy used in BIRD compares rows, irrespective of row order or repeating rows. As a baseline, we tested the performance of GPT4 and Anthropic's Claude-3.
"""
