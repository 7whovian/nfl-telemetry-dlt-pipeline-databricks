import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import current_timestamp, col, from_unixtime, round, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

# ==============================================================================
# CONFIGURACIÓN DE ORIGEN Y CONTENEDORES
# ==============================================================================
# NOTA: En DLT de nivel empresarial, el Catálogo y el Esquema de destino 
# NO se definen en el código. Se configuran en la UI del Pipeline (Target schema).
# Solo conservamos los nombres de las tablas para referenciarlas limpiamente.

bronze_table_name       = 'nfl_telemetry_uc'
silver_table_name       = 'silver_player_tracking_uc' 
gold_player_table_name  = 'gold_player_realtime_features_uc'
gold_play_table_name    = 'gold_play_trajectory_uc'

# ==============================================================================
# CONFIGURACIÓN DE CONFLUENT CLOUD (KAFKA)
# ==============================================================================
confluent_bootstrap_servers = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
confluent_topic_name        = "nfl_telemetry"

# Recuperación segura de credenciales mediante Databricks Secrets
confluent_api_key = dbutils.secrets.get(scope="kafka-scope", key="confluent_api_key")
confluent_api_secret = dbutils.secrets.get(scope="kafka-scope", key="confluent_api_secret")

# Definición del esquema JSON esperado desde el tópico de Kafka
telemetry_schema = StructType([
    StructField("game_id", StringType(), True),
    StructField("play_id", StringType(), True),
    StructField("play_status", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("position", StringType(), True),      
    StructField("team", StringType(), True),
    StructField("x_coord", DoubleType(), True),       
    StructField("y_coord", DoubleType(), True),       
    StructField("speed_mph", DoubleType(), True),
    StructField("acceleration_m_s2", DoubleType(), True),
    StructField("heart_rate_bpm", IntegerType(), True),
    StructField("stamina_pct", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# Cadena de conexión segura JAAS para Confluent
jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{confluent_api_key}' password='{confluent_api_secret}';"

# ==============================================================================
# CAPA BRONZE (STREAMING DESDE KAFKA/CONFLUENT)
# ==============================================================================
@dlt.table(
    name=bronze_table_name,
    comment="Tabla Bronze V2 ingiriendo telemetría cruda en vivo desde Confluent Cloud (Kafka)",
    table_properties={"quality": "bronze"}
)
def nfl_telemetry_bronze_v2():
    
    # Conexión al Tópico de Kafka
    df_raw = (
        spark.readStream 
        .format("kafka") 
        .option("kafka.bootstrap.servers", confluent_bootstrap_servers) 
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("subscribe", confluent_topic_name) 
        .option("startingOffsets", "latest") 
        .option("failOnDataLoss", "false")
        .load()
    )

    # Decodificación del Payload
    df_enriched = (
        df_raw 
        .withColumn("json_payload", from_json(col("value").cast("string"), telemetry_schema)) 
        .select("json_payload.*", col("timestamp").alias("kafka_timestamp"), "topic", "partition", "offset") 
        .withColumn("ingested_at", current_timestamp())
    )

    return df_enriched


# =========================================================================
# CAPA SILVER (CLEAN & INCREMENTAL)
# =========================================================================
@dlt.table(
    name=silver_table_name, 
    comment="Capa Silver de telemetría NFL: Datos limpios, tipados y deduplicados",
    cluster_by=["game_id"], # Reemplaza el particionamiento tradicional
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("velocidad_valida", "speed_mph >= 0.0 AND speed_mph <= 30.0")
@dlt.expect_or_drop("ritmo_cardiaco_valido", "heart_rate_bpm IS NOT NULL")
def create_silver_player_tracking():
    
    # Leemos la capa Bronze como STREAM (esencial al venir de Kafka)
    df_raw = dlt.read_stream(bronze_table_name)
    
    # Casteo inicial, incluyendo la conversión del timestamp (requerida para el Watermark)
    df_clean = (
        df_raw 
        .withColumn("timestamp", from_unixtime(col("timestamp") / 1000).cast("timestamp")) 
        .withColumn("speed_mph", col("speed_mph").cast("double")) 
        .withColumn("heart_rate_bpm", col("heart_rate_bpm").cast("int")) 
        .withColumn("acceleration_m_s2", round(col("acceleration_m_s2").cast("double"), 2))
    )
    
    # Manejo de estado en Streaming: Watermark permite borrar el historial viejo de memoria
    # Deduplicamos tolerando un retraso de red de hasta 1 minuto
    df_clean = (
        df_clean
        .withWatermark("timestamp", "1 minute")
        .dropDuplicates(["game_id", "play_id", "player_id", "timestamp"])
    )
    
    # Eliminamos las columnas técnicas y añadimos auditoría
    df_clean = df_clean.drop("topic", "partition", "offset", "kafka_timestamp")
    df_clean = df_clean.withColumn("silver_processed_at", current_timestamp())
    
    return df_clean


# =========================================================================
# CAPA GOLD 1: FEATURES EN TIEMPO REAL POR JUGADOR 
# =========================================================================
@dlt.table(
    name=gold_player_table_name,
    comment="One Big Table (OBT) optimizada para ML. Materialized View aislada por juego.",
    cluster_by=["game_id"]
)
def gold_player_realtime_features():
    
    # Lectura batch (Vista Materializada) recalcula eficientemente las agregaciones complejas
    df_silver = dlt.read(silver_table_name)
    
    # Jerarquía correcta en groupBy (game_id primero)
    df_gold = df_silver.groupBy("game_id", "player_id").agg(
        F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
        F.max("speed_mph").alias("max_speed_today"),
        F.round(F.avg("heart_rate_bpm"), 0).alias("avg_heart_rate"),
        F.max("heart_rate_bpm").alias("peak_heart_rate"),
        
        F.collect_list("acceleration_m_s2").alias("acceleration_history_array"),
        
        F.current_timestamp().alias("feature_timestamp")
    )
    
    return df_gold


# =========================================================================
# CAPA GOLD 2: TRAYECTORIA DE LA JUGADA 
# =========================================================================
@dlt.table(
    name=gold_play_table_name,
    comment="OBT para predecir el tipo de jugada. Agrupa la telemetría a nivel de Jugada (Play).",
    cluster_by=["game_id"]
)
def create_gold_play_trajectory():
    
    df_silver = dlt.read(silver_table_name)
    
    df_gold_play = df_silver.groupBy("game_id", "play_id").agg(
        F.collect_list(
            F.struct("player_id", "timestamp", "speed_mph", "acceleration_m_s2")
        ).alias("telemetry_history_array"),
        
        F.round(F.max("speed_mph"), 2).alias("max_speed_in_play"),
        F.countDistinct("player_id").alias("players_tracked"),
        
        F.current_timestamp().alias("feature_timestamp")
    )
    
    return df_gold_play