"""Geração de dados: simula uma pequena fábrica e publica os eventos no broker MQTT.

Linha de produção sequencial: station1 -> station2 -> ... -> station9 -> QA.
Cada estação é um recurso único (um lock), então só processa um item por vez.
Itens são lançados na linha em intervalos aleatórios, até o limite de
MAX_CONCURRENT_ITEMS itens percorrendo a fábrica ao mesmo tempo.

Execução: `python -m sensors.sensors` a partir da raiz do projeto.
As credenciais/endereço do broker MQTT vêm do .env (ver mqtt/mqtt.py).
"""
import os
import random
import threading
import time

from dotenv import load_dotenv

from mqtt.mqtt import build_client, shutdown_client, publish_event, publish_quality, publish_item

load_dotenv()

# --- Layout da linha de produção --------------------------------------------

PROCESS_STATIONS = [f"station{i}" for i in range(1, 10)]
STATIONS = PROCESS_STATIONS + ["QA"]

CYCLE_TIME_RANGES = {station: (10, 30) for station in PROCESS_STATIONS}
CYCLE_TIME_RANGES["QA"] = (8, 15)

QA_PASS_RATE = 0.90
QA_FAIL_REASONS = ["alinhamento", "peca_em_falta", "quebrado"]

MAX_CONCURRENT_ITEMS = 5
ITEM_LAUNCH_INTERVAL = (10, 30)

# Fator de aceleração da simulação (não é uma variável MQTT, só facilita
# testar sem esperar minutos por item). SIM_SPEED=1 -> tempo real.
SIM_SPEED = float(os.getenv("SIM_SPEED", "1"))

station_locks = {station: threading.Lock() for station in STATIONS}
items_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_ITEMS)

stop_event = threading.Event()

_counters_lock = threading.Lock()
_item_seq = 0
_completed_count = 0


def _next_item_id():
    global _item_seq
    with _counters_lock:
        _item_seq += 1
        return f"ITEM-{_item_seq:04d}"


def _register_completion(client):
    global _completed_count
    with _counters_lock:
        _completed_count += 1
        count = _completed_count
    publish_item(client, count)


def _run_station(client, station, item_id):
    """Ocupa a estação por um ciclo, publicando 'working' e depois 'idle'."""
    cycle_time = random.uniform(*CYCLE_TIME_RANGES[station])
    with station_locks[station]:
        publish_event(client, station, "working", item_id, cycle_time)
        time.sleep(cycle_time / SIM_SPEED)
        publish_event(client, station, "idle", item_id)


def process_item(client, item_id):
    """Percorre a linha inteira com um item: estações de processo + QA."""
    try:
        for station in PROCESS_STATIONS:
            if stop_event.is_set():
                return
            _run_station(client, station, item_id)

        _run_station(client, "QA", item_id)

        passed = random.random() < QA_PASS_RATE
        reason = None if passed else random.choice(QA_FAIL_REASONS)
        publish_quality(client, item_id, passed, reason)

        _register_completion(client)
    finally:
        items_semaphore.release()


def launch_items(client):
    """Lança itens na linha respeitando o limite de itens simultâneos."""
    threads = []
    try:
        while not stop_event.is_set():
            items_semaphore.acquire()
            item_id = _next_item_id()
            thread = threading.Thread(target=process_item, args=(client, item_id), daemon=True)
            thread.start()
            threads.append(thread)

            time.sleep(random.uniform(*ITEM_LAUNCH_INTERVAL) / SIM_SPEED)
    except KeyboardInterrupt:
        print("\nEncerrando simulação...")
        stop_event.set()

    for thread in threads:
        thread.join(timeout=2)


def run_simulation():
    client = build_client()
    try:
        launch_items(client)
    finally:
        shutdown_client(client)


if __name__ == "__main__":
    run_simulation()
