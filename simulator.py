import os
import json
import time
import signal
import random
import logging
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData

# ------------------ CONFIG ------------------
EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
PRODUCER_ID = os.getenv("PRODUCER_ID", "vehicle_simulator_1")

SEND_INTERVAL_SEC = 5
MAX_RETRIES = 3
BASE_RETRY_DELAY_SEC = 2
# --------------------------------------------

# ----------- SIMULATION CONSTANTS -----------
VEHICLE_IDS = ["V1001", "V1002", "V1003"]

SPEED_RANGE = (0, 140)            # kmph
ENGINE_TEMP_RANGE = (70, 120)     # °C
FUEL_LEVEL_RANGE = (5, 100)       # %
LATITUDE_RANGE = (12.8, 13.2)
LONGITUDE_RANGE = (77.4, 77.8)
# --------------------------------------------

# ------------------ LOGGING ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("vehicle-producer")
# --------------------------------------------

running = True

def shutdown_handler(sig, frame):
    global running
    logger.info("Shutdown signal received. Stopping producer gracefully...")
    running = False

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

def generate_vehicle_event():
    return {
        "vehicle_id": random.choice(VEHICLE_IDS),
        "producer_id": PRODUCER_ID,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "speed_kmph": random.randint(*SPEED_RANGE),
        "engine_temp_c": random.randint(*ENGINE_TEMP_RANGE),
        "fuel_level_pct": round(random.uniform(*FUEL_LEVEL_RANGE), 2),
        "latitude": round(random.uniform(*LATITUDE_RANGE), 6),
        "longitude": round(random.uniform(*LONGITUDE_RANGE), 6)
    }

def send_event_with_retry(producer, event):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer.send_batch([EventData(json.dumps(event))])
            logger.info(
                f"Event sent | vehicle_id={event['vehicle_id']} | attempt={attempt}"
            )
            return
        except Exception as e:
            logger.warning(
                f"Send failed (attempt {attempt}/{MAX_RETRIES}): {e}"
            )
            time.sleep(BASE_RETRY_DELAY_SEC * attempt)

    logger.error(
        f"Event dropped after {MAX_RETRIES} retries | vehicle_id={event['vehicle_id']}"
    )

def main():
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONN_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    logger.info("Vehicle telemetry producer started")

    with producer:
        while running:
            event = generate_vehicle_event()
            send_event_with_retry(producer, event)
            time.sleep(SEND_INTERVAL_SEC)

    logger.info("Producer stopped cleanly")

if __name__ == "__main__":
    main()
