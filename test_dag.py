from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator

GCP_CONN_ID = "Testing"
GCP_PROJ_ID = "admazes-da-intern-gcp-dev-2024"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date' : datetime(2024, 7, 15),
    'email': ['andre@admazes.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': True,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'Testing',
    default_args=default_args,
    description='calculation of DOGE coins size per transaction',
    #schedule_interval = '@hourly',
    schedule_interval="@once"
)

#Task1
T1_Query = """
SELECT
    edition
FROM
    admazes-da-intern-gcp-dev-2024.Testing.us_health
LIMIT
    10
"""
Check=BigQueryCheckOperator(
    task_id="Check_Table",
    sql=T1_Query,
    use_legacy_sql=False,
    gcp_conn_id=GCP_CONN_ID,
    dag=dag
)

#Task2
T2_Query="""
DELETE FROM
    Testing.us_health
WHERE
    subpopulation IN ("Other Race", "Multiracial")
"""
Delete_data = BigQueryInsertJobOperator(
    task_id="Delete_data",
    configuration={
        "query":{
            "query": T2_Query,
            "use_legacy_sql": False,
        }
    },
    dag=dag,
    gcp_conn_id=GCP_CONN_ID
)

#Depedencies
Check >> Delete_data
