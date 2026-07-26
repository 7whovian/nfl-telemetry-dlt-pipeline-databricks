import json
import random
import time
from confluent_kafka import Producer
from airflow.hooks.base import BaseHook
import logging
import os

# ==============================================================================
# 1. CONFIGURACIÓN SEGURA Y MODULAR
# ==============================================================================
def get_kafka_config() -> dict:
    """Extrae las credenciales seguras de la bóveda de Airflow."""
    kafka_conn = BaseHook.get_connection("confluent_kafka_default")
    return {
        'bootstrap.servers': f"{kafka_conn.host}:{kafka_conn.port}",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': kafka_conn.login,
        'sasl.password': kafka_conn.password
    }

def delivery_callback(err, msg):
    """Callback para confirmar o registrar errores en la entrega."""
    if err:
        print(f'Error al enviar: {err}')

def get_players_pool(ofensiva: str = "KC_OFF", defensiva: str = "BAL_DEF") -> list:
    """
    Retorna la lista de los 22 jugadores leyendo los rosters desde el archivo JSON externo.
    """
    # 1. Construir la ruta absoluta de forma dinámica y segura
    # __file__ hace referencia a la ubicación de ESTE script (nfl_telemetry_sintetic_data.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 2. Apuntar a la subcarpeta 'players_data_def' donde guardaste el JSON
    json_path = os.path.join(current_dir, 'players_data_def', 'nfl_rosters.json')
    
    # 3. Lectura segura del archivo utilizando Context Manager (with) y manejo de excepciones
    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            rosters = json.load(file)
    except FileNotFoundError:
        logging.error(f"Error CRÍTICO: No se encontró el archivo de rosters en la ruta: {json_path}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error CRÍTICO: El archivo JSON está mal formado o tiene un error de sintaxis. Detalles: {e}")
        return []

    # 4. Validar que los equipos solicitados realmente existan en el diccionario del JSON
    if ofensiva not in rosters:
        logging.error(f"Error: El equipo ofensivo '{ofensiva}' no existe en el JSON.")
        return []
    if defensiva not in rosters:
        mensaje_error = f"Error: El equipo defensivo '{defensiva}' no existe en el JSON."
        logging.error(mensaje_error)
        raise ValueError(mensaje_error)

    # 5. Inyectar los jugadores en el pool (11 Off + 11 Def = 22)
    jugadores_en_cancha = rosters[ofensiva] + rosters[defensiva]
    
    return jugadores_en_cancha
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# ==============================================================================
# 2. MOTOR DEL PARTIDO CONTINUO (ENCAPSULADO)
# ==============================================================================
def run_telemetry_simulation(
    minutos_de_simulacion: float = 5.0, 
    topic: str = 'nfl_telemetry', 
    ofensiva: str = "KC_OFF", 
    defensiva: str = "BAL_DEF"
):
    """
    Función principal que será llamada por el DAG de Airflow.
    Ejecuta el bucle de streaming y maneja la conexión a Kafka de forma aislada.
    """
    # 1. Generación dinámica del game_id basado en los parámetros
    # Ejemplo: Si envías KC_OFF y BAL_DEF, el game_id será "2026_01_KC_BAL"
    equipo_off = ofensiva.split('_')[0]
    equipo_def = defensiva.split('_')[0]
    game_id = f"2026_01_{equipo_off}_{equipo_def}"

    # 2. Inyección dinámica del equipo en el pool
    players_pool = get_players_pool(ofensiva=ofensiva, defensiva=defensiva)

    # 3. Validación Fail-Fast: Si hubo un error en la lectura del JSON, no iniciamos Kafka
    if not players_pool:
        logging.error("Abortando simulación: No se pudo cargar el pool de jugadores.")
        return

    # 4. Inicialización de Kafka
    conf = get_kafka_config()
    producer = Producer(conf)

    logging.info(f"🏈 Iniciando simulación de {minutos_de_simulacion} minutos para el juego: {game_id}...")

    numero_jugada_actual = 101
    estado_partido = "HUDDLE"
    tiempo_proximo_cambio = time.time() + 5
    tiempo_fin_simulacion = time.time() + (minutos_de_simulacion * 60)

    try:
        while time.time() < tiempo_fin_simulacion:
            current_time_ms = int(time.time() * 1000)

            # Lógica de cambio de estado (Jugada vs Descanso)
            if time.time() >= tiempo_proximo_cambio:
                if estado_partido == "HUDDLE":
                    estado_partido = "ACTIVO"
                    duracion_fase = random.randint(5, 12)
                    logging.info(f"🟢 ¡SNAP! Inicia play_{numero_jugada_actual} (Duración: {duracion_fase}s)")
                else:
                    estado_partido = "HUDDLE"
                    duracion_fase = random.randint(25, 40)
                    logging.info(f"🛑 Fin de la jugada. Equipos en Huddle (Duración: {duracion_fase}s)")
                    numero_jugada_actual += 1

                tiempo_proximo_cambio = time.time() + duracion_fase

            # Generación de datos condicionada por el estado
            if estado_partido == "ACTIVO":
                current_play_id = f"play_{numero_jugada_actual}"
                rango_velocidad = (5.0, 21.5)
                rango_aceleracion = (-4.0, 5.0)
                movimiento = 1.5
            else:
                current_play_id = f"huddle_pre_{numero_jugada_actual}"
                rango_velocidad = (0.0, 3.5)
                rango_aceleracion = (-1.0, 1.0)
                movimiento = 0.2

            # Envío continuo de telemetría de los 22 jugadores
            for player in players_pool:
                player["x"] = max(0.0, min(120.0, player["x"] + random.uniform(-movimiento, movimiento)))
                player["y"] = max(0.0, min(53.3, player["y"] + random.uniform(-movimiento, movimiento)))

                telemetry_data = {
                    "game_id": game_id,
                    "play_id": current_play_id,
                    "play_status": estado_partido,
                    "player_id": player["id"],
                    "player_name": player["name"],
                    "position": player["pos"],
                    "team": player["team"],
                    "x_coord": round(player["x"], 2),
                    "y_coord": round(player["y"], 2),
                    "speed_mph": round(random.uniform(*rango_velocidad), 1),
                    "acceleration_m_s2": round(random.uniform(*rango_aceleracion), 2),
                    "heart_rate_bpm": random.randint(130 if estado_partido == "HUDDLE" else 150, 185),
                    "stamina_pct": round(random.uniform(65.0, 98.0), 1),
                    "timestamp": current_time_ms
                }

                producer.produce(topic, key=player["id"], value=json.dumps(telemetry_data), callback=delivery_callback)

            producer.poll(0)
            
            # Transmisión ininterrumpida a 5Hz
            time.sleep(0.2)

        logging.info(f"✅ Tiempo de simulación ({minutos_de_simulacion} min) completado exitosamente.")

    except Exception as e:
        logging.error(f"⚠️ Error o interrupción en la simulación: {e}")
    finally:
        # Garantiza que todos los mensajes en cola se envíen antes de cerrar
        producer.flush()
        logging.info("Simulador cerrado de forma segura.")

# Este bloque asegura que el script no se ejecute accidentalmente al ser importado
if __name__ == "__main__":
    # Puedes probar diferentes equipos localmente aquí sin alterar el DAG
    run_telemetry_simulation(
        minutos_de_simulacion=1.0, 
        ofensiva="KC_OFF", 
        defensiva="SF_DEF"
    )