import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import timedelta, datetime
import os
from airflow.providers.snowflake.operators.snowflake import SnowflakeQueryOperator



SLACK_WEBHOOK_TOKEN = "O1w4orLYxkcrkb2Kn5lWgI31"


def success_slack_alert(context):
    slack_msg = f""":white_check_mark: DAG *{context['dag'].dag_id}* se ejecutó exitosamente."""
    return SlackWebhookOperator(
        task_id="slack_success_notification",
        http_conn_id=None,
        webhook_token=SLACK_WEBHOOK_TOKEN,
        message=slack_msg,
        username="airflow"
    ).execute(context=context)


def failure_slack_alert(context):
    slack_msg = f""":x: DAG *{context['dag'].dag_id}* falló.\n*Error:* `{context.get('exception')}`"""
    return SlackWebhookOperator(
        task_id="slack_failure_notification",
        http_conn_id=None,
        webhook_token=SLACK_WEBHOOK_TOKEN,
        message=slack_msg,
        username="airflow"
    ).execute(context=context)


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "on_success_callback": success_slack_alert,
    "on_failure_callback": failure_slack_alert
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
    schedule='*/15 * * * *',
    start_date=pendulum.now('UTC').subtract(days=1),
    catchup=False,
    tags=['snowflake', 'dynamic_tables'],
) as dag:

    generate_sql = PythonOperator(
        task_id='generate_sql',
        python_callable=generate_sql_statements,
        do_xcom_push=True
    )

    snowflake_task = SnowflakeQueryOperator(
        task_id="run_snowflake_query",
        sql="{{ ti.xcom_pull(task_ids='generate_sql') }}",
        snowflake_conn_id="TSMDCQB-NNC51870"
    )

    generate_sql >> snowflake_task

