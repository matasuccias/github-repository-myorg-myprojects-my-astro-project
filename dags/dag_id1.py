import pendulum
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import airflow.providers.snowflake
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import os
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    "on_success_callback": success_slack_alert,
    "on_failure_callback": failure_slack_alert
}
def success_slack_alert(context):
    slack_msg = f""":white_check_mark: DAG *{context['dag'].dag_id}* se ejecutó exitosamente."""
    return SlackWebhookOperator(
        task_id="slack_success_notification",
        http_conn_id=None,
        webhook_token=os.environ.get("https://hooks.slack.com/services/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"),
        message=slack_msg,
        username="airflow"
    ).execute(context=context)

# Función para enviar notificación a Slack en caso de error
def failure_slack_alert(context):
    slack_msg = f""":x: DAG *{context['dag'].dag_id}* falló.
*Error:* `{context.get('exception')}`"""
    return SlackWebhookOperator(
        task_id="slack_failure_notification",
        http_conn_id=None,
        webhook_token=os.environ.get("https://hooks.slack.com/services/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"),
        message=slack_msg,
        username="airflow"
    ).execute(context=context)
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

snowflake_task = SnowflakeOperator(
    task_id="run_snowflake_query",
    sql="SELECT CURRENT_DATE;",
    snowflake_conn_id="TSMDCQB-NNC51870"  # Asegúrate de que sea tu conn ID válido
)

    generate_sql >> create_dynamic_tables

