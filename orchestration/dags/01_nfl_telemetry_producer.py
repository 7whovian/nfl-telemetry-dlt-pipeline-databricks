import time
import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.models.param import Param
import logging
import os

# Importamos tu función modular
from module.nfl_telemetry_sintetic_data import run_telemetry_simulation

# ==============================================================================
# CONFIGURACIONES GLOBALES
# ==============================================================================
local_tz = pendulum.timezone("America/Mexico_City")
PIPELINE_ID = 'eaf2033a-a87e-4e63-8675-c1b23909ae75'
DATABRICKS_CONN_ID = 'databricks_default'

# ==============================================================================
# GESTIÓN DE ALERTAS (NIVEL EMPRESARIAL)
# ==============================================================================
def on_failure_alert(context):
    """
    Función llamada cuando falla una tarea.
    En producción, aquí se integra Slack/Teams/Email (ej. SlackWebhookOperator).
    """
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    exec_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    
    mensaje_alerta = (
        f"🚨 ALERTA CRÍTICA DE PIPELINE 🚨\n"
        f"DAG: {dag_id}\n"
        f"Tarea Fallida: {task_id}\n"
        f"Fecha de Ejecución: {exec_date}\n"
        f"Revisa los logs aquí: {log_url}"
    )
    # Mostramos la alerta en el log maestro. En un entorno real, enviaríamos este string al webhook.
    logging.error(mensaje_alerta)

# ==============================================================================
# FUNCIONES DE CONTROL DE DATABRICKS
# ==============================================================================
def start_dlt_pipeline(**kwargs):
    """Enciende el pipeline DLT en modo continuo gestionando excepciones."""
    try:
        hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
        databricks_conn = hook.get_connection(DATABRICKS_CONN_ID)
        url = f"{databricks_conn.host}/api/2.0/pipelines/{PIPELINE_ID}/updates"
        headers = {"Authorization": f"Bearer {databricks_conn.password}"}
        
        # Timeout de 15s para dar margen a la red, pero proteger el worker de Airflow
        response = requests.post(url, headers=headers, json={"full_refresh": False}, timeout=15)
        response.raise_for_status()
        logging.info("Databricks DLT encendido. Iniciando aprovisionamiento...")
    except requests.exceptions.RequestException as e:
        logging.error(f"Fallo crítico de red al contactar la API de Databricks: {e}")
        raise # Elevamos el error para que Airflow lo maneje (reintentos o fallo)

def wait_for_provisioning(**kwargs):
    """Pausa controlada para garantizar que el clúster esté activo antes de inyectar datos"""
    minutos_espera = 6
    logging.info(f"Pausando DAG por {minutos_espera} minutos mientras Databricks levanta el cómputo serverless...")
    time.sleep(minutos_espera * 60)
    logging.info("Tiempo de espera finalizado. El clúster debería estar listo para consumir.")

def wait_for_processing(**kwargs):
    """Pausa de drenaje para permitir que Databricks termine de consumir y consolidar la cola de Kafka"""
    minutos_drenaje = 3
    logging.info(f"Simulación enviada. Esperando {minutos_drenaje} minutos para drenar la cola de Kafka y consolidar las tablas Delta...")
    time.sleep(minutos_drenaje * 60)
    logging.info("Drenaje completado. Procediendo con el apagado seguro del pipeline.")

def stop_dlt_pipeline(**kwargs):
    """Apaga el pipeline DLT liberando el clúster con manejo de errores."""
    try:
        hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
        databricks_conn = hook.get_connection(DATABRICKS_CONN_ID)
        url = f"{databricks_conn.host}/api/2.0/pipelines/{PIPELINE_ID}/stop"
        headers = {"Authorization": f"Bearer {databricks_conn.password}"}
        
        response = requests.post(url, headers=headers, timeout=15)
        response.raise_for_status()
        logging.info("Databricks DLT apagado exitosamente. Recursos liberados.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error al intentar apagar DLT. ATENCIÓN: Posible clúster huérfano. Detalles: {e}")
        raise 

# ==============================================================================
# DEFINICIÓN DEL DAG
# ==============================================================================
default_args = {
    'owner': 'jm',
    'retries': 2,                    # RETRIES: Intentará hasta 2 veces adicionales si una tarea falla transitoriamente
    'retry_delay': pendulum.duration(minutes=1), # Espera 1 minuto entre reintentos para no saturar la API
    'on_failure_callback': on_failure_alert # ALERTA: Notifica inmediatamente en caso de fallo final
}

with DAG(
    dag_id='02_nfl_end_to_end_streaming_test',
    default_args=default_args,
    start_date=pendulum.datetime(2026, 7, 23, tz=local_tz),
    schedule=None, 
    catchup=False,
    tags=['nfl', 'streaming', 'databricks', 'confluent'],
    
    # ESTO ES CRÍTICO: Permite que Airflow envíe un Float real en lugar de un String a tu función
    render_template_as_native_obj=True, 
    
    params={
        "equipo_defensivo": Param(
            default="BAL_DEF",
            type="string",
            description="Selecciona la defensiva rival contra la que jugará KC_OFF",
            enum=[
                "BAL_DEF", "LV_DEF", "BUF_DEF", "DET_DEF","LAR_DEF",
                "CIN_DEF", "MIA_DEF", "PHI_DEF", "SF_DEF", "DAL_DEF",
                "CLE_DEF", "PIT_DEF", "HOU_DEF",
                "NYJ_DEF", "SEA_DEF", "GB_DEF","TB_DEF","NO_DEF","ATL_DEF","CHI_DEF","NE_DEF"
            ] 
        ),
        "minutos_de_simulacion": Param(
            default=2.0,
            type="number", 
            description="Duración de inyección de datos en minutos. (Máximo seguro: 30 min para proteger costos)",
            minimum=0.5,   
            maximum=15.0   
        )
    }
) as dag:

    start_consumer = PythonOperator(
        task_id='start_databricks_dlt',
        python_callable=start_dlt_pipeline,
        # Si la API de DBX falla, queremos reintentar el encendido
        retries=3,
        retry_delay=pendulum.duration(seconds=30)
    )

    wait_cluster_warmup = PythonOperator(
        task_id='wait_for_cluster_provisioning',
        python_callable=wait_for_provisioning
    )

    stream_synthetic_data = PythonOperator(
        task_id='stream_data_to_confluent',
        python_callable=run_telemetry_simulation,
        op_kwargs={
            'minutos_de_simulacion': '{{ params.minutos_de_simulacion }}', 
            'topic': 'nfl_telemetry',
            'ofensiva': 'KC_OFF',
            'defensiva': '{{ params.equipo_defensivo }}' 
        },
        # No reintentar la simulación si falla a medias, podría duplicar mensajes en Kafka
        retries=0 
    )

    wait_dlt_processing = PythonOperator(
        task_id='wait_for_dlt_processing',
        python_callable=wait_for_processing
    )

    stop_consumer = PythonOperator(
        task_id='stop_databricks_dlt',
        python_callable=stop_dlt_pipeline,
        trigger_rule='all_done', # FUNDAMENTAL: Se ejecuta siempre, incluso si stream_synthetic_data falló (por el raise de KeyError de la defensiva)
        # Si falla el comando de apagado, reintentamos fuertemente para evitar costos huérfanos
        retries=4,
        retry_delay=pendulum.duration(minutes=2)
    )

    # ==============================================================================
    # ORDEN DE EJECUCIÓN (GRAFO)
    # ==============================================================================
    start_consumer >> wait_cluster_warmup >> stream_synthetic_data >> wait_dlt_processing >> stop_consumer