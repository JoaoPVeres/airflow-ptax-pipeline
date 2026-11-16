from pendulum import datetime
from datetime import timedelta
from pathlib import Path
import json

from airflow.sdk import dag, task, Variable, get_current_context, BaseHook
from airflow.sdk.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from sgs.client import buscar_serie
from ptax.storage import salvar_json, ler_json
from pipeline.assets import ASSET_BRONZE_SELIC
from pipeline.callbacks import notificar_falha, notificar_retry

SQL_UPSERT = Path("/opt/airflow/include/sql/bronze/upsert_selic_raw.sql")

@dag(
    dag_id="selic_ingest",
    schedule=CronDataIntervalTimetable("@daily", timezone="America/Sao_Paulo"),
    start_date=datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=50),
    default_args={
        "owner": "joao",
        "retries": 2,
        "retry_delay" : timedelta(seconds=30),
        "on_retry_callback": notificar_retry,
        "on_failure_callback" : notificar_falha
    },
    template_searchpath=["/opt/airflow/include/sql"],
    tags=["bronze", "selic"]
)
def selic_ingest():
    criar_selic_raw = SQLExecuteQueryOperator(
            task_id="criar_selic_raw",
            conn_id="postgres_dw",
            sql="bronze/create_selic_raw.sql"
    )
    @task.python(task_id="extrair", 
                multiple_outputs=False,
                pool="api_bcb",
                retries=3,
                retry_delay=timedelta(minutes=1),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(minutes=10),
                execution_timeout=timedelta(minutes=2))
    def extrair() -> dict:
        context = get_current_context()

        inicio = context["data_interval_start"]
        if inicio is None:
            raise ValueError("Nao possui data_interval_start")
        data = inicio.in_timezone("America/Sao_Paulo").date()

        serie = int(Variable.get("selic_serie"))
        base_path = Variable.get("bronze_base_path")
        conexao = BaseHook.get_connection("http_bcb_sgs")
        base_url = f"https://{conexao.host}"

        dados = buscar_serie(serie, data, data, base_url)
        caminho = salvar_json(dados, base_path, f"SGS{serie}", data, dataset="selic")

        return {"serie": serie, "data_ref": data.isoformat(), "arquivo": caminho,
                "qtd_registros" : len(dados), "source_url" : base_url}

    @task.python(
            task_id="carregar",
            multiple_outputs=False, 
            execution_timeout=timedelta(minutes=2), 
            outlets=[ASSET_BRONZE_SELIC]
            )
    def carregar(meta: dict) -> dict:
        context = get_current_context()
        dados = ler_json(meta["arquivo"])
        sql = SQL_UPSERT.read_text(encoding="utf-8")
        parametros = {
            "serie": meta["serie"],
            "data_ref": meta["data_ref"],
            "payload": json.dumps(dados, ensure_ascii=False),
            "qtd_registros": meta["qtd_registros"],
            "source_url": meta["source_url"],
            "arquivo_origem": meta["arquivo"],
            "run_id": context["run_id"],
        }
        hook = PostgresHook(postgres_conn_id="postgres_dw")
        hook.run(sql, parameters=parametros, autocommit=True)

        if meta["qtd_registros"] == 0: 
            raise AirflowSkipException(F"Dia sem Selic: {meta["data_ref"]}")
                    
        return {"serie": meta["serie"], "data_ref": meta["data_ref"], "qtd_registros": meta["qtd_registros"]}

    meta = extrair()
    criar_selic_raw >> meta 
    carregar(meta)

selic_ingest()