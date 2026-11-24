from pendulum import datetime

from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="setup_dw",
    schedule=None,
    start_date=datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    template_searchpath=["/opt/airflow/include/sql"],
    tags=["setup"]
    )
def setup_dw():
    SQLExecuteQueryOperator(
        task_id="criar_audit",
        conn_id="postgres_dw",
        sql="audit/create_audit.sql",
        split_statements=True
    )

setup_dw()