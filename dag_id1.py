
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_sql_statements():
    suffix = datetime.now().strftime('%Y%m%d_%H%M')
    tables = ['EVENTS', 'GROUP_TOPICS', 'GROUPS', 'MEMBERS_TOPICS', 'TOPICS', 'VENUES']
    sql_statements = []

    for table in tables:
        new_table = f"{table}_{suffix}"
        sql = f"CREATE TABLE IF NOT EXISTS TOPICS.PUBLIC.{new_table} LIKE TOPICS.PUBLIC.{table};"
        sql_statements.append(sql)

    return " ".join(sql_statements)

with DAG(
    dag_id='create_snowflake_tables_every_15min',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['snowflake', 'dynamic_tables'],
) as dag:

    create_dynamic_tables = SnowflakeOperator(
        task_id='create_dynamic_tables',
        sql=generate_sql_statements(),
        snowflake_conn_id='snowflake_conn_id', 
    )
