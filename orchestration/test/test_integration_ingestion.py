import os
import json
import pytest
import boto3
from confluent_kafka import Producer, Consumer
from dotenv import load_dotenv

# Forzar la carga del .env desde la carpeta 'orchestration'
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=env_path)

# ... (el resto del código continúa igual)
# ------------------------------------------------------------------
# FIXTURES: Datos de prueba estandarizados para la NFL
# ------------------------------------------------------------------
@pytest.fixture
def mock_telemetry_event():
    """Genera un evento de telemetría con la estructura requerida por el pipeline."""
    return {
        "game_id": 2026072900,
        "play_id": 9999,
        "player_id": 12345,
        "x_position": 45.2,
        "y_position": 24.8,
        "speed": 18.5,
        "event": "integration_test_ping"
    }

# ------------------------------------------------------------------
# PRUEBA 1: Integración con Amazon S3 (Vía Batch/Landing)
# ------------------------------------------------------------------
def test_s3_ingestion_path(mock_telemetry_event):
    """Verifica permisos de escritura y lectura directa en el bucket de S3."""
    s3_client = boto3.client('s3')
    bucket_name = os.getenv("AWS_S3_BUCKET_NAME", "nfl-telemetry-landing-bucket")
    test_key = "staging/test_integration_payload.json"

    # 1. Intentar escribir el evento de prueba
    put_response = s3_client.put_object(
        Bucket=bucket_name,
        Key=test_key,
        Body=json.dumps(mock_telemetry_event),
        ContentType="application/json"
    )
    assert put_response["ResponseMetadata"]["HTTPStatusCode"] == 200, "Fallo al escribir en Amazon S3"

    # 2. Verificar que el objeto existe realmente en S3
    head_response = s3_client.head_object(Bucket=bucket_name, Key=test_key)
    assert head_response["ContentLength"] > 0, "El archivo subido a S3 está vacío"

    # 3. Limpieza (Borrar el objeto de prueba para no ensuciar la capa Bronce)
    s3_client.delete_object(Bucket=bucket_name, Key=test_key)


# ------------------------------------------------------------------
# PRUEBA 2: Integración con Confluent Kafka (Vía Streaming)
# ------------------------------------------------------------------
def test_kafka_streaming_path(mock_telemetry_event):
    """Verifica conectividad y entrega de mensajes en el tópico de Kafka."""
    kafka_config = {
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        'security.protocol': os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")    }

    # Si usas Confluent Cloud, se agregan las credenciales SASL
    if os.getenv("KAFKA_SASL_USERNAME"):
        kafka_config.update({
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv("KAFKA_SASL_USERNAME"),
            'sasl.password': os.getenv("KAFKA_SASL_PASSWORD")
        })

    producer = Producer(kafka_config)
    test_topic = os.getenv("KAFKA_TOPIC_NAME", "nfl_telemetry")
    delivery_report = {}

    def ack_callback(err, msg):
        if err is not None:
            delivery_report['error'] = str(err)
        else:
            delivery_report['success'] = True

    # 1. Publicar mensaje
    producer.produce(
        topic=test_topic,
        key=str(mock_telemetry_event["player_id"]),
        value=json.dumps(mock_telemetry_event),
        callback=ack_callback
    )
    
    # 2. Esperar confirmación del cluster (timeout de 5 segundos)
    producer.flush(timeout=5)

    assert 'error' not in delivery_report, f"Error de conexión/entrega en Kafka: {delivery_report.get('error')}"
    assert delivery_report.get('success') is True, "El mensaje no fue confirmado por el Broker de Kafka"