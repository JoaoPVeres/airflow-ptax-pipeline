from pendulum import datetime
from datetime import timedelta
import pendulum

from airflow.sdk import dag, task, Param, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from pipeline.callbacks import notificar_falha, notificar_retry

MAX_DIAS = 60

@dag(
    dag_id="pipeline_reprocessamento",
    schedule=None,
    start_date=datetime(2026, 11,1 , tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    params={
        "dag_alvo": Param(
            "bronze_ptax_ingest",
            enum=["bronze_ptax_ingest", "selic_ingest"],
            description="Bronze que será reprocessada",
        ),
        "data_inicial": Param("2026-11-12", type="string", format="date"),
        "data_final": Param("2026-11-13", type="string", format="date"),
    },
    default_args={
        "owner": "joao",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "on_retry_callbak": notificar_retry,
        "on_failure_callbak": notificar_falha
    },
    tags=["orquestacao"]
)
def pipeline_reprocessamento():

    @task.python(task_id="listar_dias")
    def listar_dias() ->list[str]:
        params = get_current_context()["params"]
        inicio = pendulum.parse(params["data_inicial"]).date()
        fim = pendulum.parse(params["data_final"]).date()

        if fim < inicio:
            raise ValueError("data_final anterior a data_inicial")
        if (fim - inicio).days > MAX_DIAS:
            raise ValueError(f"Janeal maior que {MAX_DIAS} dias")

        logical_dates = []
        dia = inicio
        while dia <= fim:
            if dia.weekday() < 5:
                logical = pendulum.datetime(dia.year, dia.month, dia.day, 12, tz="America/Sao_Paulo").add(days=1)
                logical_dates.appen(logical.isoformat())
            dia += timedelta(days=1)

        if not logical_dates:
            raise ValueError("Nenhum dia util no intervalo informado")
        print(f"{len(logical_dates)} run(s) a disparar")
        return logical_dates

    TriggerDagRunOperator.partial(
        task_id="disparar_dia",
        trigger_dag_id="{{ params.dag_alvo }}",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=10,
        reset_dag_run=True,
        max_active_tis_per_dag=1
    ).expand(logical_date=listar_dias())

pipeline_reprocessamento()