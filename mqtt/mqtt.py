import os
from dotenv import load_dotenv
import dotenv
import random
import datetime
from zoneinfo import ZoneInfo
import paho.mqtt.client as paho
from paho import mqtt

load_dotenv()

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = os.getenv("MQTT_PORT")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
CLIENT_ID = f"simulation-{random.randint(0, 1000)}"
TOPIC_BASE = "fabrica/ativo"
TOPIC_QA = "fabrica/qualidade"
TOPIC_ITEM = "fabrica/itens"

connected = False

def on_connect(client, userdata, flags, rc, properties=None):
    global connected
    if rc == 0:
        print(f"Connected ao Broker MQTT: {MQTT_BROKER} (ClientID: {CLIENT_ID})")
        connected = True
    else:
        print(f"Falha ao se conectar, código de retorno: {rc}")
        connected = False

def on_disconnect(client, userdata, rc, properties=None):
    global connected
    print("Desligado do broker")
    connected = False

def publish_event(client, station, status, cycle_time=None):
    if status == "working":
        if station == "QA":
            power_kwh = random.uniform(0.3, 0.5)
        else:
            power_kwh = random.uniform(0.7, 1.1)
    else:
        power_kwh = 0

    data = {
        'station': station,
        'status': status,
        'power_consumption_kw': round(power_kwh, 4),
        'timestamp': datetime.datetime.now(ZoneInfo("America/Fortaleza")).isoformat()
    }