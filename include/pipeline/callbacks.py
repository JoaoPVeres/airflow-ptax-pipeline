import logging

logger = logging.getLogger(__name__)


def notificar_falha(context) -> None:
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
    ti = context["ti"]
    logger.info(
        "SUCESSO | dag=%s task=%s run=%s",
        ti.dag_id,
        ti.task_id,
        context.get("run_id"),
    )