import json
from kafka import KafkaConsumer
import snowflake.connector

# Connect directly to Snowflake RAW schema
conn = snowflake.connector.connect(
    user='NOMESH16',
    password='Ishant@1234567',
    account='ibaayxn-vj30510',
    warehouse='COMPUTE_WH',
    database='ATMOSYNC_DB',
    schema='RAW'
)
cursor = conn.cursor()

consumer = KafkaConsumer(
    'container-telemetry',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Kafka-to-Snowflake Consumer running. Ingesting live streams...")

for message in consumer:
    telemetry_payload = message.value
    query = "INSERT INTO ATMOSYNC_DB.RAW.RAW_CONTAINER_TELEMETRY (raw_payload) SELECT PARSE_JSON(%s)"
    cursor.execute(query, (json.dumps(telemetry_payload),))
    print(f"Ingested to Snowflake RAW: {telemetry_payload['container_id']} @ {telemetry_payload['timestamp']}")