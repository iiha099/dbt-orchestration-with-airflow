import os

from airflow import DAG

# IMPORT YOUR PACKAGES HERE


DBT_DIR = os.getenv("DBT_DIR")


with DAG(
    "dbt_basics",
    # YOUR CODE HERE
) as dag:
    pass  # YOUR CODE HERE
