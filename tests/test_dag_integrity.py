import pytest

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # versões diferentes organizam o módulo de outro jeito
    from airflow.models.dagbag import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="/opt/airflow/dags", include_examples=False)


def test_nenhuma_dag_com_erro_de_importacao(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors


def test_toda_dag_tem_tags(dagbag):
    sem_tag = [dag_id for dag_id, dag in dagbag.dags.items() if not dag.tags]
    assert not sem_tag, f"DAGs sem tags: {sem_tag}"


def test_toda_task_tem_retries(dagbag):
    sem_retry = [
        f"{dag_id}.{task.task_id}"
        for dag_id, dag in dagbag.dags.items()
        for task in dag.tasks
        if (task.retries or 0) < 1
    ]
    assert not sem_retry, f"Tasks sem retries: {sem_retry}"