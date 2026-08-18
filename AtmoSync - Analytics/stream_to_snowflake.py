import json
import time
import random
from datetime import datetime
import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='NOMESH16',
    password='Ishant@1234567',
    account='ibaayxn-vj30510',
    warehouse='COMPUTE_WH',
    database='ATMOSYNC_DB',
    schema='RAW'
)
cursor = conn.cursor()

CONTAINERS = [
    {"id": "CONT-A101", "commodity": "Avocados", "origin": "Guayaquil", "dest": "Rotterdam", "base_temp": 4.0},
    {"id": "CONT-B202", "commodity": "Bananas", "origin": "Costa Rica", "dest": "Hamburg", "base_temp": 13.0}
]

print("Streaming real-time telemetry to Snowflake RAW schema...")
while True:
    container = random.choice(CONTAINERS)
    temp_drift = random.choice([0.0, 0.2, 3.8 if random.random() < 0.3 else 0.1])
    
    payload = {
        "event_id": f"EVT-{random.randint(100000, 999999)}",
        "container_id": container["id"],
        "commodity": container["commodity"],
        "timestamp": datetime.utcnow().isoformat(),
        "telemetry": {
            "temperature_c": round(container["base_temp"] + temp_drift, 2),
            "humidity_pct": round(random.uniform(80.0, 95.0), 1),
            "vibration_g": round(random.uniform(0.01, 0.45), 3)
        },
        "location": {
            "latitude": round(random.uniform(10.0, 50.0), 4),
            "longitude": round(random.uniform(-70.0, 10.0), 4)
        }
    }
    
    query = "INSERT INTO ATMOSYNC_DB.RAW.RAW_CONTAINER_TELEMETRY (raw_payload) SELECT PARSE_JSON(%s)"
    cursor.execute(query, (json.dumps(payload),))
    print(f"Ingested: {payload['container_id']} | Temp: {payload['telemetry']['temperature_c']}°C")
    time.sleep(3)