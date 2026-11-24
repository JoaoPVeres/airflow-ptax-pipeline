from pendulum import datetime
from datetime import timedelta

from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from pipeline.assets import ASSET_SILVER_PTAX, ASSET_SILVER_SELIC, ASSET_GOLD_PTAX
from pipeline.callbacks import notificar_falha, notificar_retry

@dag(
    dag_id="gold_ptax_metrics",
    schedule=[ASSET_SILVER_PTAX, ASSET_SILVER_SELIC],
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
    tags=["gold", "ptax", "silver"]
)
def gold_ptax_metrics():
    criar_tabelas_gold = SQLExecuteQueryOperator(
        task_id="criar_tabelas_gold",
        conn_id="postgres_dw",
        sql="gold/create_gold.sql",
        split_statements=True
    )

    atualizar_ptax_diaria = SQLExecuteQueryOperator(
        task_id="atualizar_ptax_diaria",
        conn_id="postgres_dw",
        sql="gold/ptax_diaria.sql",
        params={"janela_media": 5}
    )
    atualizar_ptax_vs_selic = SQLExecuteQueryOperator(
        task_id="atualizar_ptax_vs_selic",
        conn_id="postgres_dw",
        sql="gold/ptax_vs_selic.sql",
        outlets=[ASSET_GOLD_PTAX]
    )

    criar_tabelas_gold >> atualizar_ptax_diaria >> atualizar_ptax_vs_selic

gold_ptax_metrics()
