from pendulum import datetime
from pathlib import Path
from datetime import timedelta

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from pipeline.assets import ASSET_BRONZE_PTAX, ASSET_SILVER_PTAX
from pipeline.callbacks import notificar_falha, notificar_retry, notificar_sucesso

SQL_DIR = Path("/opt/airflow/include/sql")

@dag(
    dag_id="silver_ptax_transform",
    schedule=[ASSET_BRONZE_PTAX],
    start_date=datetime(2026,11,1, tz="America/Sao_Paulo"),
    catchup=False,
    template_searchpath=["/opt/airflow/include/sql"],
    max_active_runs=1,
    default_args={
        "owner": "joao",
        "retries": 2,
        "retry_delay" : timedelta(seconds=30),
        "on_retry_callback": notificar_retry,
        "on_failure_callback" : notificar_falha
    },
    tags=["silver", "ptax"]
)
def silver_ptax_transform():
    criar_tabelas_silver = SQLExecuteQueryOperator(
            task_id="criar_tabelas_silver",
            conn_id="postgres_dw",
            sql="silver/create_ptax.sql",
            split_statements=True
    )

    @task.branch(task_id="checar_qualidade")
    def checar_qualidade() -> str:
        sql = (SQL_DIR / "silver" / "contar_invalidos.sql").read_text(encoding="utf-8")
        linha = PostgresHook(postgres_conn_id="postgres_dw").get_first(sql)
        contagem = linha[0]
        print(f"Registro invalidos: {contagem}")
        if contagem > 0:
             return "isolar_quarentena"
        return "sem_pendencias"

    isolar_quarentena = SQLExecuteQueryOperator(
        task_id="isolar_quarentena",
        conn_id="postgres_dw",
        sql="silver/isolar_quarentena.sql"
    )

    sem_pendencias = EmptyOperator(task_id="sem_pendencias", on_success_callback=notificar_sucesso)

    upsert_silver = SQLExecuteQueryOperator(
        task_id="upsert_silver",
        conn_id="postgres_dw",
        sql="silver/upsert_ptax.sql",
        split_statements=True,
        trigger_rule="none_failed_min_one_success",
        outlets=[ASSET_SILVER_PTAX]
    )

    criar_tabelas_silver >> checar_qualidade() >> [isolar_quarentena, sem_pendencias] >> upsert_silver

silver_ptax_transform()