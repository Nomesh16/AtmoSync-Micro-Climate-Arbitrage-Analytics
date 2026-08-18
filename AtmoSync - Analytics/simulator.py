import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Connect to Kafka running in Docker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CONTAINERS = [
    {"id": "CONT-A101", "commodity": "Avocados", "origin": "Guayaquil", "dest": "Rotterdam", "base_temp": 4.0},
    {"id": "CONT-B202", "commodity": "Bananas", "origin": "Costa Rica", "dest": "Hamburg", "base_temp": 13.0},
    {"id": "CONT-C303", "commodity": "Berries", "origin": "Agadir", "dest": "London", "base_temp": 2.0}
]

def generate_telemetry():
    container = random.choice(CONTAINERS)
    
    # Simulate occasional micro-climate anomaly (temperature spike)
    temp_drift = random.choice([0.0, 0.2, 0.5, 3.5 if random.random() < 0.15 else 0.1])
    
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
    return payload

if __name__ == "__main__":
    print("Starting AtmoSync IoT Telemetry Producer...")
    while True:
        data = generate_telemetry()
        producer.send('container-telemetry', value=data)
        print(f"Sent: {data['container_id']} | Temp: {data['telemetry']['temperature_c']}°C")
        time.sleep(2)  