import os
import pytest
from airflow.models.dagbag import DagBag

@pytest.fixture(scope="session")
def dag_bag():
    """
    Carga todos los DAGs ubicados en la carpeta orchestration/dags.
    include_examples=False evita cargar los DAGs de muestra predeterminados de Airflow.
    """
    dags_folder = os.path.join(os.path.dirname(__file__), "../dags")
    return DagBag(dag_folder=dags_folder, include_examples=False)

def test_no_import_errors(dag_bag):
    """
    Verifica que no existan errores de sintaxis, importaciones rotas
    o dependencias faltantes en ninguno de tus DAGs.
    """
    assert len(dag_bag.import_errors) == 0, f"Se encontraron errores al importar los DAGs:\n{dag_bag.import_errors}"

def test_dags_have_tasks(dag_bag):
    """
    Verifica que todos los DAGs detectados tengan al menos una tarea configurada.
    """
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.tasks) > 0, f"El DAG '{dag_id}' está vacío (no tiene tareas asociadas)."

def test_dags_has_valid_owner(dag_bag):
    """
    (Buena práctica) Asegura que ningún DAG se quede con el owner por defecto 'airflow'.
    """
    for dag_id, dag in dag_bag.dags.items():
        assert dag.owner != "airflow", f"El DAG '{dag_id}' debe definir un owner personalizado en sus default_args."
