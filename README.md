 # AtmoSync: Real-Time Cargo Preservation & Arbitrage Analytics

AtmoSync is an end-to-end streaming data engineering and real-time operational analytics pipeline. It ingests simulated IoT micro-climate sensor streams (temperature, humidity, GPS coordinates) from cold-chain shipping containers, streams events through Apache Kafka, stages raw telemetry in Snowflake, performs automated degradation modeling using dbt, and serves dynamic arbitrage and rerouting dashboards in Apache Superset.

---

## 🏗️ System Architecture

```
[ IoT Container Telemetry ]
            │
            ▼
   [ Apache Kafka ] (Producer / Broker / Zookeeper)
            │
            ▼
[ Python Streaming Consumer ]
            │
            ▼
[ Snowflake Data Cloud ] (RAW.RAW_CONTAINER_TELEMETRY)
            │
            ▼
      [ dbt Core ] (ANALYTICS.FCT_SPOILAGE_ARBITRAGE)
            │
            ▼
  [ Apache Superset ] (Live Micro-Climate & Arbitrage Dashboard)
```

1. **Ingestion Layer:** Python Kafka producer generates streaming container events.
2. **Message Broker:** Apache Kafka with Zookeeper orchestrates the telemetry event stream.
3. **Consumer & Staging:** Multi-threaded Python consumer batches records into Snowflake (`RAW.RAW_CONTAINER_TELEMETRY`).
4. **Transformation Layer:** dbt models classify micro-climate drift, predict spoilage risk, and calculate financial arbitrage salvage values.
5. **Serving & BI Layer:** Apache Superset visualizes real-time KPI alerts, geospatial coordinates via deck.gl scatterplots, and an arbitrage decision matrix.

---

## 🚀 Tech Stack

* **Streaming:** Apache Kafka, Zookeeper
* **Data Warehouse:** Snowflake
* **Data Modeling:** dbt Core (`dbt-snowflake`)
* **Visualization & BI:** Apache Superset, deck.gl
* **Languages & Runtimes:** Python 3.10+, SQL, PowerShell
* **Containerization:** Docker & Docker Compose

---

## 📂 Project Structure

```text
AtmoSync - analytics/
├── atmosync_dbt/                   # dbt transformation models
│   ├── models/
│   │   ├── staging/                # Staging views from raw telemetry
│   │   └── marts/                  # fct_spoilage_arbitrage fact tables
│   ├── dbt_project.yml
│   └── profiles.yml
├── docker-compose.yml              # Kafka, Zookeeper, & Superset container stack
├── kafka_producer.py               # IoT telemetry simulator
├── kafka_to_snowflake_consumer.py  # Kafka to Snowflake raw staging consumer
├── requirements.txt                # Python dependencies
└── README.md
```

---

## 🐳 Docker Infrastructure (`docker-compose.yml`)

Ensure persistent storage for Apache Superset so dashboard layouts and credentials persist across container restarts:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  superset:
    image: apache/superset:latest
    container_name: superset
    ports:
      - "8088:8088"
    volumes:
      - superset_home:/app/superset_home
    environment:
      SUPERSET_SECRET_KEY: "atmosync_secret_key_production_12345"

volumes:
  superset_home:
```

---

## ❄️ Snowflake Configuration & Connection Details

### Snowflake Parameters
* **Account Identifier:** `ibaayxn-vj30510`
* **User:** `NOMESH16`
* **Warehouse:** `COMPUTE_WH`
* **Database:** `ATMOSYNC_DB`
* **Target Schema:** `ANALYTICS` (and `RAW`)
* **Role:** `ACCOUNTADMIN`

### Superset SQLAlchemy Connection URI
Paste this string under **Settings > Database Connections > + Database** in Superset:
```text
snowflake://NOMESH16:<YOUR_PASSWORD>@ibaayxn-vj30510/ATMOSYNC_DB/ANALYTICS?warehouse=COMPUTE_WH&role=ACCOUNTADMIN
```

### Python Connector Code (`kafka_to_snowflake_consumer.py`)
```python
import snowflake.connector

conn = snowflake.connector.connect(
    user='NOMESH16',
    password='YOUR_ACTUAL_SNOWFLAKE_PASSWORD',
    account='ibaayxn-vj30510',
    warehouse='COMPUTE_WH',
    database='ATMOSYNC_DB',
    schema='RAW'
)
```

---

## 🛠️ Step-by-Step Initial Setup

### 1. Set Up Python Virtual Environment
```powershell
cd "D:\Infotact solutions\Project\AtmoSync - analytics"
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Start Docker Containers
```powershell
docker compose up -d
```

### 3. Install Snowflake Driver & Initialize Superset
```powershell
# Install Snowflake SQLAlchemy driver inside Superset container
docker exec -it --user root superset /app/.venv/bin/python -m ensurepip --upgrade
docker exec -it --user root superset /app/.venv/bin/python -m pip install --no-cache-dir snowflake-sqlalchemy

# Initialize DB metadata and Admin User
docker exec -it superset superset db upgrade
docker exec -it superset superset fab create-admin --username admin --firstname Admin --lastname User --email admin@atmosync.io --password admin
docker exec -it superset superset init
docker restart superset
```

---

## ⚡ Execution Runbook (Starting the Pipeline)

### Terminal 1: Launch Kafka-to-Snowflake Consumer
```powershell
cd "D:\Infotact solutions\Project\AtmoSync - analytics"
.\venv\Scripts\Activate.ps1
python kafka_to_snowflake_consumer.py
```

### Terminal 2: Launch IoT Telemetry Producer
```powershell
cd "D:\Infotact solutions\Project\AtmoSync - analytics"
.\venv\Scripts\Activate.ps1
python kafka_producer.py
```

### Terminal 3: Execute dbt Transformations
```powershell
cd "D:\Infotact solutions\Project\AtmoSync - analytics\atmosync_dbt"
..\venv\Scripts\Activate.ps1
dbt run
```

---

## 📊 Dashboard Architecture & Layout

Access Superset at `http://localhost:8088` (Credentials: `admin` / `admin`).

### Dashboard Title
`AtmoSync - Cargo Preservation & Arbitrage Operations`

### Chart Specifications

| Chart Name | Type | Key Fields / Metrics | Layout Position |
| :--- | :--- | :--- | :--- |
| **Critical Spoilage Alerts** | Big Number | Count of containers with `THERMAL_STATUS = 'CRITICAL'` | Row 1 (Top Left, 6 cols) |
| **Prevented Financial Loss** | Big Number with Trendline | `SUM(ARBITRAGE_LOSS_PREVENTED)` | Row 1 (Top Right, 6 cols) |
| **Live Fleet Micro-Climate Map** | deck.gl Scatterplot | Longitude/Latitude, Color: `THERMAL_STATUS`, Radius: `25000` meters | Row 2 (Middle, 12 cols) |
| **Reroute & Arbitrage Decision Matrix** | Table | `CONTAINER_ID`, `COMMODITY`, `TEMPERATURE_C`, `QUALITY_DEGRADATION_PCT`, `THERMAL_STATUS`, `RECOMMENDED_REROUTE_MARKET`, `SALVAGE_VALUE_PER_TON`, `ARBITRAGE_LOSS_PREVENTED` (Sort: `MAX(ARBITRAGE_LOSS_PREVENTED)` DESC) | Row 3 (Bottom, 12 cols) |

* **Live Streaming:** Enable **Set auto-refresh interval** to `10 seconds` or `30 seconds` via the dashboard menu (`...`).

---

## 🛑 Pipeline Teardown (Stopping the Project)

1. **Stop Python Streaming Jobs:**
   Press `Ctrl + C` inside Terminal 1 (`kafka_to_snowflake_consumer.py`) and Terminal 2 (`kafka_producer.py`).

2. **Stop Docker Infrastructure Without Data Loss:**
   ```powershell
   # Stops all services while preserving SQLite dashboard metadata in superset_home volume
   docker compose stop
   # or
   docker compose down
   ```

---

## 📜 License
This project is licensed under the MIT License.
