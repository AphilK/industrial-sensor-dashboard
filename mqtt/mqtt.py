"""Cliente MQTT: conexão com o broker e publicação dos eventos da fábrica.

Todas as variáveis de conexão (host, porta, usuário, senha) vêm do .env.
"""
import json
import os
import random
import time
import datetime
from zoneinfo import ZoneInfo

import paho.mqtt.client as paho
from dotenv import load_dotenv

load_dotenv()

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))

CLIENT_ID = f"simulation-{random.randint(0, 1000)}"

TOPIC_BASE = "fabrica/ativo"
TOPIC_QA = "fabrica/qualidade"
TOPIC_ITEM = "fabrica/itens"

TIMEZONE = ZoneInfo("America/Fortaleza")

connected = False


def _now_iso():
    return datetime.datetime.now(TIMEZONE).isoformat()


def on_connect(client, userdata, flags, rc, properties=None):
    global connected
    if rc == 0:
        print(f"Conectado ao Broker MQTT: {MQTT_BROKER}:{MQTT_PORT} (ClientID: {CLIENT_ID})")
        connected = True
    else:
        print(f"Falha ao se conectar, código de retorno: {rc}")
        connected = False


def on_disconnect(client, userdata, rc, properties=None):
    global connected
    print("Desconectado do broker")
    connected = False


def build_client(connect_timeout=10):
    """Cria, autentica e conecta o client MQTT usando as variáveis do .env."""
    client = paho.Client(
        callback_api_version=paho.CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
        protocol=paho.MQTTv5,
    )

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
    client.loop_start()

    waited = 0.0
    while not connected and waited < connect_timeout:
        time.sleep(0.1)
        waited += 0.1

    if not connected:
        raise ConnectionError(
            f"Não foi possível conectar ao broker MQTT {MQTT_BROKER}:{MQTT_PORT} "
            f"em {connect_timeout}s"
        )

    return client


def shutdown_client(client):
    client.loop_stop()
    client.disconnect()


def publish_event(client, station, status, item_id=None, cycle_time=0):
    """Publica o status de uma estação (working/idle) em fabrica/ativo/<estacao>."""
    if status == "working":
        power_kwh = random.uniform(0.3, 0.5) if station == "QA" else random.uniform(0.7, 1.1)
    else:
        power_kwh = 0

    data = {
        "station": station,
        "status": status,
        "item_id": item_id,
        "power_consumption_kw": round(power_kwh, 3),
        "timestamp": _now_iso(),
    }

    if status == "idle":
        data["cycle_time"] = 0.0
    elif cycle_time:
        data["cycle_time"] = round(cycle_time, 3)

    topic = f"{TOPIC_BASE}/{station}"
    payload = json.dumps(data)
    result = client.publish(topic, payload)

    if result[0] == 0:
        suffix = f" (cycle: {cycle_time:.2f}s)" if cycle_time else ""
        print(f"[{station}] {item_id or ''} -> {status}{suffix}")
    else:
        print(f"[{station}] ERRO ao publicar")


def publish_quality(client, item_id, passed, reason=None):
    """Publica o resultado do controle de qualidade em fabrica/qualidade."""
    data = {
        "item_id": item_id,
        "result": "aprovado" if passed else "reprovado",
        "reason": reason,
        "timestamp": _now_iso(),
    }

    payload = json.dumps(data)
    result = client.publish(TOPIC_QA, payload)

    if result[0] == 0:
        print(f"[QA] {item_id} -> {data['result']}" + (f" ({reason})" if reason else ""))
    else:
        print("[QA] ERRO ao publicar")


def publish_item(client, count):
    """Publica a contagem de itens produzidos em fabrica/itens."""
    data = {
        "count": count,
        "timestamp": _now_iso(),
    }

    payload = json.dumps(data)
    result = client.publish(TOPIC_ITEM, payload)

    if result[0] == 0:
        print(f"[ITENS] total produzido: {count}")
    else:
        print("[ITENS] ERRO ao publicar")
