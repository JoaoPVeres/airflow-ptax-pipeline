from pendulum import datetime
from datetime import timedelta
from pathlib import Path
import json
import requests

from airflow.sdk import dag, task, task_group, Variable, get_current_context, BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from ptax.client import buscar_cotacao
from ptax.storage import salvar_json, ler_json
from pipeline.assets import ASSET_BRONZE_PTAX
from pipeline.callbacks import notificar_falha, notificar_retry, notificar_sucesso

SQL_UPSERT = Path("/opt/airflow/include/sql/bronze/upsert_ptax_raw.sql")

@dag(
    dag_id="bronze_ptax_ingest",
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
    tags=["bronze", "ptax"]
)
def bronze_ptax_ingest():

    @task.sensor(
            task_id="api_disponivel",
            poke_interval=30,
            timeout=300,
            mode="reschedule"
    )
    def api_disponivel() -> bool:
        conexao = BaseHook.get_connection("http_bcb_ptax")
        url = (f"https://{conexao.host}"
            "/olinda/servico/PTAX/versao/v1/odata/Moedas?$top=1&$format=json"
        )
        try:
            respota = requests.get(url, timeout=10)
        except requests.RequestException as e:
            print(f"API indisponivel: {e}")
            return False
        print(f"Status API: {respota.status_code}")
        return respota.status_code == 200

    @task.python(task_id="listar_moedas")
    def listar_moedas() -> list[str]:
        moedas = Variable.get("ptax_moedas", deserialize_json=True)
        if not isinstance(moedas, list) or not moedas:
            raise ValueError(f"Variavel invalida: {moedas!r}")
        return moedas

    @task.python(task_id="extrair", 
                multiple_outputs=False,
                pool="api_bcb",
                retries=3,
                retry_delay=timedelta(minutes=1),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(minutes=10),
                execution_timeout=timedelta(minutes=2))
    def extrair(moeda: str) -> dict:
        context = get_current_context()

        inicio = context["data_interval_start"]
        if inicio is None:
            raise ValueError("Nao possui data_interval_start")
        data = inicio.in_timezone("America/Sao_Paulo").date()

        base_path = Variable.get("bronze_base_path")
        conexao = BaseHook.get_connection("http_bcb_ptax")
        base_url = f"https://{conexao.host}"

        dados = buscar_cotacao(moeda, data, base_url)
        caminho = salvar_json(dados, base_path, moeda, data)

        return {"moeda": moeda, "data_ref": data.isoformat(), "arquivo": caminho,
                "qtd_registros" : len(dados["value"]), "source_url" : base_url}

    @task.python(task_id="carregar", multiple_outputs=False, execution_timeout=timedelta(minutes=2))
    def carregar(meta: dict) -> dict:
        context = get_current_context()
        dados = ler_json(meta["arquivo"])
        sql = SQL_UPSERT.read_text(encoding="utf-8")
        parametros = {
            "moeda": meta["moeda"],
            "data_ref": meta["data_ref"],
            "payload": json.dumps(dados, ensure_ascii=False),
            "qtd_registros": meta["qtd_registros"],
            "source_url": meta["source_url"],
            "arquivo_origem": meta["arquivo"],
            "run_id": context["run_id"],
        }
        hook = PostgresHook(postgres_conn_id="postgres_dw")
        hook.run(sql, parameters=parametros, autocommit=True)

        return {"moeda": meta["moeda"], "data_ref": meta["data_ref"], "qtd_registros": meta["qtd_registros"]}

    @task_group(group_id="ingestao_moeda")
    def ingestao_moeda(moeda:str):
        meta = extrair(moeda)
        return carregar(meta)

    @task.branch(task_id="decidir_fluxo")
    def decidir_fluxo(resultados) -> str:
        total = sum(r["qtd_registros"] for r in resultados)
        print(f"Total registros: {total}")
        if total > 0:
            return "dia_com_cotacao"
        return "dia_sem_cotacao"

    @task.python(task_id="dia_sem_cotacao")
    def dia_sem_cotacao() -> None:
        context = get_current_context()
        data = context["data_interval_start"].in_timezone("America/Sao_Paulo").date()
        print(f"Data sem cotacao: {data}")

    com_cotacao = EmptyOperator(task_id="dia_com_cotacao", outlets=[ASSET_BRONZE_PTAX])
    fim_bronze = EmptyOperator(task_id="fim_bronze", trigger_rule="none_failed_min_one_success", on_success_callback=notificar_sucesso)

    moedas = listar_moedas()
    api_disponivel() >> moedas
    resultados = ingestao_moeda.expand(moeda=moedas)
    ramo = decidir_fluxo(resultados)

    ramo >> [com_cotacao, dia_sem_cotacao()] >> fim_bronze

bronze_ptax_ingest()