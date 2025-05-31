import pendulum
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

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
    schedule='*/15 * * * *',  # <--- aquí el cambio
    start_date=pendulum.now('UTC').subtract(days=1),
    catchup=False,
    tags=['snowflake', 'dynamic_tables'],
) as dag:

    generate_sql = PythonOperator(
        task_id='generate_sql',
        python_callable=generate_sql_statements,
    )

    create_dynamic_tables = SnowflakeSqlApiOperator(
        task_id='create_dynamic_tables',
        sql="{{ ti.xcom_pull(task_ids='generate_sql') }}",
        snowflake_conn_id='snowflake_conn_id',
    )

    generate_sql >> create_dynamic_tables

