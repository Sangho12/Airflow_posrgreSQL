from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator

default_args = {
    "owner" : "airflow",
    "email" : 'andre@admazes.com',
    "email_on_retry" : False,
    "email_on_failure" : False,
    "retries" : 2,
    "retry_delay" : timedelta(minutes=5)
    }
dag = DAG(
    'checking_logic',
    default_args=default_args,
    description='creation_of_table_to_test',
    #schedule_interval = '@hourly',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
)

#create table 1
t1 = SQLExecuteQueryOperator(
     task_id="create_table",
    conn_id='test',
    sql=f"""
        CREATE TABLE IF NOT EXISTS birds (
            bird_name VARCHAR,
            observation_year INT,
            bird_happiness INT
        );
        """,
    )

#create table 2
t2 = SQLExecuteQueryOperator(
     task_id="create_table_null",
    conn_id='test',
    sql=f"""
        CREATE TABLE IF NOT EXISTS birds2 (
            bird_name VARCHAR,
            observation_year INT,
            bird_happiness INT
        );
   """,
)

#insert data into table2
t3 = SQLExecuteQueryOperator(
    task_id='insert_data2',
    conn_id='test',
    sql='''
        INSERT INTO birds2 (bird_name, observation_year, bird_happiness)
        SELECT * FROM (VALUES
         (NULL::VARCHAR, NULL::INTEGER, NULL::INTEGER),
            (NULL::VARCHAR, NULL::INTEGER, NULL::INTEGER),
            (NULL::VARCHAR, NULL::INTEGER, NULL::INTEGER),
         (NULL::VARCHAR, NULL::INTEGER, NULL::INTEGER),
         ('test_name', NULL::INTEGER, NULL::INTEGER),
            (NULL::VARCHAR, NULL::INTEGER, NULL::INTEGER)
        ) AS new_data (bird_name, observation_year, bird_happiness)
        WHERE NOT EXISTS (
            SELECT 1 FROM birds2
            WHERE (birds2.bird_name IS NOT DISTINCT FROM new_data.bird_name)
             AND (birds2.observation_year IS NOT DISTINCT FROM new_data.observation_year)
             AND (birds2.bird_happiness IS NOT DISTINCT FROM new_data.bird_happiness)
);

    ''',
    dag=dag,
)      


#insert data into table
t4 = SQLExecuteQueryOperator(
    task_id='insert_data',
    conn_id='test',
    sql='''
        INSERT INTO birds (bird_name, observation_year, bird_happiness)
        SELECT * FROM (VALUES
            ('King vulture (Sarcoramphus papa)', 2022-05-16, 9),
            ('Victoria Crowned Pigeon (Goura victoria)', 2021-04-15, 10),
            ('Orange-bellied parrot (Neophema chrysogaster)', 2021-03-14, 9),
            ('Orange-bellied parrot (Neophema chrysogaster)', 2020-02-13, 8),
            (NULL, 2019, 8),
            ('Indochinese green magpie (Cissa hypoleuca)', 2018-01-12, 10)
        ) AS new_data (bird_name, observation_year, bird_happiness)
        WHERE NOT EXISTS (
            SELECT 1 FROM birds
            WHERE (birds.bird_name IS DISTINCT FROM new_data.bird_name)
              AND (birds.observation_year IS DISTINCT FROM new_data.observation_year)
              AND (birds.bird_happiness IS DISTINCT FROM new_data.bird_happiness)
        );
    ''',
    dag=dag,
)      

#select specific value with collate function (select, cast, collate)
t5 = SQLExecuteQueryOperator(
    task_id='check_and_collate_data',
    conn_id='test',
    sql='''
        SELECT 
            CAST(bird_name COLLATE "ucs_basic" AS VARCHAR(255))
        FROM birds
        WHERE observation_year = '2021';
    ''',
    dag=dag,
)

#select specific value (string split)
t6 = SQLExecuteQueryOperator(
    task_id='string_split',
    conn_id='test',
    sql='''
        WITH split_values AS (
            SELECT unnest(string_to_array(
                (SELECT bird_name FROM birds WHERE observation_year = '2021' LIMIT 1), 
                ' ')) AS bird
        )
        SELECT bird FROM split_values;
    ''',
    dag=dag,
)

#date diff (where, in datediff)
t7 = SQLExecuteQueryOperator(
    task_id='calculate_date_diff',
    conn_id='test',
    sql='''
        SELECT 
            bird_name,
            observation_year,
            (CURRENT_DATE - MAKE_DATE(observation_year, 1, 1))::INTEGER AS days_since_observation
        FROM birds;
    ''',
    do_xcom_push=False,
    dag=dag,
)

#union table
t8 = SQLExecuteQueryOperator(
    task_id='union_table_new',
    conn_id='test',
    sql='''
CREATE TABLE IF NOT EXISTS U_birds AS
SELECT 
    COALESCE(birds.bird_name, birds2.bird_name) AS bird_name, 
    COALESCE(birds.observation_year, birds2.observation_year) AS observation_year,
    COALESCE(birds.bird_happiness, birds2.bird_happiness) AS bird_happiness
FROM birds
FULL OUTER JOIN birds2
ON birds.bird_name = birds2.bird_name AND birds.observation_year = birds2.observation_year;

    ''',
)

t1 >> t4 >> t5 >> t6 >> t7
t2 >> t3 

[t4,t3] >> t8