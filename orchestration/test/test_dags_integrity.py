import os
import pytest
from airflow.models import DagBag

# Apuntamos a la carpeta donde viven tus DAGs
DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")

@pytest.fixture(scope="session")
def dag_bag():
    # DagBag carga todos los DAGs en memoria como lo haría Airflow
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)

def test_no_import_errors(dag_bag):
    """Falla si algún DAG tiene errores de sintaxis o dependencias rotas"""
    assert len(dag_bag.import_errors) == 0, f"Errores de importación encontrados: {dag_bag.import_errors}"

def test_dags_have_valid_structure(dag_bag):
    """Valida que todos los DAGs tengan un ID y al menos una tarea"""
    for dag_id, dag in dag_bag.dags.items():
        assert dag_id is not None
        assert len(dag.tasks) > 0, f"El DAG {dag_id} no tiene tareas definidas"