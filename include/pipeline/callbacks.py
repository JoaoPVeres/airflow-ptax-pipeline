import logging

logger = logging.getLogger(__name__)

SQL_INSERT = """
INSERT INTO audit.pipeline_eventos
    (dag_id, task_id, map_index, run_id, evento, tentativa, mensagem)
VALUES
    (%(dag_id)s, %(task_id)s, %(map_index)s, %(run_id)s, %(evento)s, %(tentativa)s, %(mensagem)s)
"""

def _registrar(context, evento: str) -> None:
    """Grava o evento em audit. Nunca deixa uma falha de auditoria derrubar a task."""
    try:
        # Import dentro da função: este arquivo é importado a cada parse das DAGs
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        ti = context["ti"]
        excecao = context.get("exception")
        parametros = {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "map_index": getattr(ti, "map_index", None),
            "run_id": context.get("run_id"),
            "evento": evento,
            "tentativa": getattr(ti, "try_number", None),
            "mensagem": str(excecao)[:500] if excecao else None,
        }
        PostgresHook(postgres_conn_id="postgres_dw").run(
            SQL_INSERT, parameters=parametros, autocommit=True
        )
        logger.info("AUDITORIA | %s | %s.%s", evento, parametros["dag_id"], parametros["task_id"])
    except Exception:
        logger.exception("Falha ao gravar auditoria (evento=%s)", evento)

def notificar_falha(context) -> None:
    _registrar(context, "falha")
    ti = context["ti"]
    logger.error(
        "FALHA | dag=%s task=%s map_index=%s run=%s tentativa=%s erro=%s",
        ti.dag_id,
        ti.task_id,
        ti.map_index,
        context.get("run_id"),
        ti.try_number,
        context.get("exception"),
    )


def notificar_retry(context) -> None:
    _registrar(context, "retry")
    ti = context["ti"]
    logger.warning(
        "RETRY | dag=%s task=%s map_index=%s tentativa=%s erro=%s",
        ti.dag_id,
        ti.task_id,
        ti.map_index,
        ti.try_number,
        context.get("exception"),
    )


def notificar_sucesso(context) -> None:
    _registrar(context, "sucesso")
    ti = context["ti"]
    logger.info(
        "SUCESSO | dag=%s task=%s run=%s",
        ti.dag_id,
        ti.task_id,
        context.get("run_id"),
    )