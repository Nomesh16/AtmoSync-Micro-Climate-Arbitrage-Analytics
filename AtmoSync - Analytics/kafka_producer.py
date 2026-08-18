import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CONTAINERS = [
    {"id": "CONT-A101", "commodity": "Avocados", "base_temp": 4.0},
    {"id": "CONT-B202", "commodity": "Bananas", "base_temp": 13.0},
    {"id": "CONT-C303", "commodity": "Strawberries", "base_temp": 2.0},
    {"id": "CONT-D404", "commodity": "Mangos", "base_temp": 10.0}
]

print("Publishing real-time container telemetry to Kafka topic 'container-telemetry'...")

while True:
    container = random.choice(CONTAINERS)
    
    # 25% chance of a high temperature excursion
    is_spike = random.random() < 0.25
    temp_offset = random.uniform(3.5, 7.0) if is_spike else random.uniform(-0.5, 0.5)
    
    payload = {
        "event_id": f"EVT-{random.randint(100000, 999999)}",
        "container_id": container["id"],
        "commodity": container["commodity"],
        "timestamp": datetime.utcnow().isoformat(),
        "telemetry": {
            "temperature_c": round(container["base_temp"] + temp_offset, 2),
            "humidity_pct": round(random.uniform(75.0, 95.0), 1),
            "vibration_g": round(random.uniform(0.01, 0.50), 3)
        },
        "location": {
            "latitude": round(random.uniform(35.0, 45.0), 4),
            "longitude": round(random.uniform(-10.0, 15.0), 4)
        }
    }
    
    producer.send('container-telemetry', value=payload)
    print(f"Sent event {payload['event_id']} | Container: {payload['container_id']} | Temp: {payload['telemetry']['temperature_c']}°C")
    time.sleep(2)