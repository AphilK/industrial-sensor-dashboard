import time
import random
import datetime
import json
import threading

connected = False

CYCLE_TIME_RANGES = {
    "station1": (10, 30),
    "station2": (10, 30),
    "station3": (10, 30),
    "station4": (10, 30),
    "station5": (10, 30),
    "station6": (10, 30),
    "station7": (10, 30),
    "station8": (10, 30),
    "station9": (10, 30),
    "QA": (8, 15)
}

STATIONS = ["station1", "station2", "station3", "station4", "station5", "station6", "station7", "station8", "station9", "QA"]

QA_PASS_RATE = 0.90
QA_FAIL_REASONS = ["alinhamento", "peca_em_falta", "quebrado"]

MAX_CONCURRENT_ITEMS = 5
ITEM_LAUNCH_INTERVAL = (10, 30)

station_locks = {station: threading.Lock() for station in STATIONS}