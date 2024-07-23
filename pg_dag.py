from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator, SQLExecuteQueryOperator

connection_id = "pg_connection"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date' : datetime(2024, 7, 15),
    'email': ['andre@admazes.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': True,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG(
    'pg_testing',
    default_args=default_args,
    description='pg_airflow_testing',
    #schedule_interval = '@hourly',
    schedule_interval="@once"
)

Task1_check = SQLTableCheckOperator(
    task_id = "check",
    conn_id = connection_id,
    table = "matches_1",
    checks ={
        "row_count_check": {
            "check_statement": "COUNT(*) = 12026"
        }
    },
    dag = dag
)

Task2_Delete = SQLExecuteQueryOperator(
    task_id = "Delete",
    conn_id = connection_id,
    sql = """
DELETE FROM
    matches_1
WHERE
    season_end_year = 1993
""",
    dag = dag
)

Task3_check = SQLTableCheckOperator(
    task_id = "post_delete_check",
    conn_id = connection_id,
    table = "matches_1",
    checks ={
        "row_count_check": {
            "check_statement": "COUNT(*) = 11564"
        }
    },
    dag = dag
)

Task4_Add_Column = SQLExecuteQueryOperator(
    task_id = "Add_column",
    conn_id = connection_id,
    sql = """
ALTER TABLE
    matches_1
ADD COLUMN
    Concat_column TEXT
""",
    dag = dag
)

Task5_Update_Concat = SQLExecuteQueryOperator(
    task_id = "Update_Concat",
    conn_id = connection_id,
    sql = """
UPDATE
    matches_1
SET
    Concat_column = CONCAT(home_team, home_goals)
""",
    dag = dag
)

Task1_check >> Task2_Delete >> Task3_check >> Task4_Add_Column >> Task5_Update_Concat