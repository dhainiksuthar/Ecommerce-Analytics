# 📊 Ecommerce Analytics

A real-time and batch data engineering pipeline that simulates an e-commerce platform, ingesting clickstream, order, and inventory events through Apache Kafka, processing them with Apache Spark Structured Streaming into a Delta Lake bronze layer, transforming data through dbt models (staging → intermediate → marts), orchestrated end-to-end by Apache Airflow, and visualized via Metabase dashboards.

## 🏗️ Architecture

```
┌──────────────────┐     ┌─────────────┐     ┌──────────────────────────┐
│  Event Generator │────▶│  Apache     │────▶│  Spark Structured        │
│  (Python/Faker)  │     │  Kafka      │     │  Streaming               │
└──────────────────┘     └─────────────┘     └────────────┬─────────────┘
                              │                           │
                              │                    Write to Delta Lake
                              │                    (Bronze Layer)
                         ┌────┴────┐                      │
                         │Kafka UI │              ┌───────▼────────┐
                         │ :8083   │              │   dbt Models   │
                         └─────────┘              │ (source →      │
                                                  │  staging →     │
                                                  │  intermediate →│
                                                  │  marts)        │
                                                  └───────┬────────┘
                                                          │
                         ┌─────────────┐          ┌───────▼────────┐
                         │  Metabase   │◀─────────│  Spark Thrift  │
                         │  :3000      │          │  Server        │
                         └─────────────┘          └────────────────┘
                                 
                        ┌───────────────────┐
                        │  Apache Airflow   │  (Orchestrates everything)
                        │  :8080            │
                        └───────────────────┘
```

### Data Flow

1. **Real-time (Streaming):** The event generator produces realistic e-commerce events (clickstream, orders, inventory) and publishes them to Kafka topics. Spark Structured Streaming jobs consume these events and write them to Delta Lake tables in the **bronze layer** (raw, append-only).

2. **Batch (Transformations):** dbt reads from the bronze Delta Lake tables via Spark Thrift Server and transforms the data through a layered model architecture — **source → staging → intermediate → marts** — producing clean, analytics-ready datasets.

3. **Orchestration:** Apache Airflow orchestrates both the Spark Streaming jobs (via DockerOperator) and the dbt transformations (via Astronomer Cosmos), with the dbt DAG scheduled to run twice daily.

4. **Visualization:** Metabase connects to the Spark Thrift Server to query the transformed data and power interactive dashboards.

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Event Generation | Python, Faker | Simulate realistic e-commerce events |
| Message Broker | Apache Kafka (KRaft mode) | Real-time event streaming |
| Kafka Monitoring | Kafbat Kafka UI | Monitor topics, consumers, messages |
| Stream Processing | Apache Spark 3.5.0 (Structured Streaming) | Consume Kafka topics and write to Delta Lake |
| Storage Format | Delta Lake 3.0.0 | ACID-compliant lakehouse storage |
| Batch Transformations | dbt (dbt-spark 1.9.0) | SQL-based data modeling and transformations |
| SQL Engine | Spark Thrift Server (HiveServer2) | JDBC/ODBC access to Delta tables for dbt and Metabase |
| Orchestration | Apache Airflow (CeleryExecutor) | DAG scheduling and task management |
| Visualization | Metabase | Interactive analytics dashboards |
| Task Queue | Redis 7.2 | Celery broker for Airflow |
| Metadata DB | PostgreSQL 16 | Airflow metadata and Metabase app database |
| Containerization | Docker & Docker Compose | Service orchestration and reproducibility |

## 📁 Project Structure

```
Ecommerce-Analytics/
├── airflow/                    # Airflow home directory
│   ├── dags/                   # Airflow DAG definitions
│   │   ├── ecommerce_spark_streaming_dag.py   # Triggers Spark Streaming jobs via DockerOperator
│   │   └── ecommerce_dbt_dag.py               # Runs dbt models via Astronomer Cosmos
│   ├── config/                 # Airflow configuration (airflow.cfg, auto-generated)
│   ├── logs/                   # Airflow task logs (auto-generated, gitignored)
│   └── plugins/                # Custom Airflow plugins (if any)
│
├── event-generator/            # Kafka event producer
│   └── producer.py             # Generates realistic clickstream, order, and inventory events
│                                 with data quality issues (nulls, duplicates, late events)
│
├── spark_streaming/            # Spark Structured Streaming application
│   ├── __init__.py
│   ├── jobs/                   # Individual streaming job scripts
│   │   ├── clickstream_to_bronze.py   # Consumes 'clickstream' topic → Delta Lake
│   │   ├── orders_to_bronze.py        # Consumes 'orders' topic → Delta Lake
│   │   └── inventory_to_bronze.py     # Consumes 'inventory' topic → Delta Lake
│   ├── schemas/                # PySpark schema definitions
│   │   └── bronze.py           # StructType schemas for clickstream, orders, and inventory
│   └── utils/                  # Shared utilities
│       ├── __init__.py
│       ├── spark_session.py    # SparkSession builder with Delta Lake configuration
│       └── kafka_utils.py      # Kafka connection helpers
│
├── dbt_project/                # dbt project for batch transformations
│   ├── dbt_project.yml         # dbt project configuration
│   ├── profiles.yml            # Connection profile (Spark Thrift Server)
│   └── models/                 # dbt SQL models organized by layer
│       ├── source/             # Source layer — reads raw Delta tables from bronze
│       │   ├── src_clickstream.sql
│       │   ├── src_orders.sql
│       │   ├── src_inventory.sql
│       │   └── schema.yml
│       ├── staging/            # Staging layer — light cleaning and standardization
│       │   └── stg_clickstream.sql
│       ├── intermediate/       # Intermediate layer — business logic joins and aggregations
│       └── marts/              # Marts layer — final analytics-ready tables
│           └── dim_events_by_traffic.sql
│
├── data/                       # Data storage (populated at runtime)
│   ├── bronze/                 # Bronze layer Delta Lake tables
│   │   ├── clickstream/        # Raw clickstream events (Delta format)
│   │   ├── orders/             # Raw order events (Delta format)
│   │   └── inventory/          # Raw inventory events (Delta format)
│   └── checkpoints/            # Spark Structured Streaming checkpoint directories
│       ├── clickstream/
│       ├── orders/
│       └── inventory/
│
├── Dockerfile                  # Airflow + event-producer image (extends apache/airflow:latest)
├── Dockerfile.spark            # Spark image with Delta Lake and Kafka JARs
├── docker-compose.yaml         # Full stack service definitions
├── requirements.txt            # Python dependencies for Airflow and event generator
├── requirements.spark.txt      # Python dependencies for Spark image
├── .env                        # Environment variables (Airflow UID, image version, etc.)
├── .gitignore                  # Git ignore rules
├── .dockerignore               # Docker build ignore rules
└── README.md                   # This file
```

### Folder Details

#### `airflow/`
The Airflow home directory mounted into all Airflow containers. Contains two DAGs:
- **`ecommerce_spark_streaming_dag.py`** — Triggers three parallel Spark Streaming jobs (clickstream, orders, inventory) using the `DockerOperator`. Each task launches a container from the `ecommerce-spark:latest` image. This DAG is manually triggered and runs indefinitely.
- **`ecommerce_dbt_dag.py`** — Runs dbt models using Astronomer Cosmos (`DbtRunLocalOperator` → `DbtTestLocalOperator`). Scheduled to run twice daily at 06:00 and 18:00 UTC.

#### `event-generator/`
A Python-based Kafka producer that generates realistic e-commerce events. It simulates:
- **Clickstream events** — Page views, searches, product views, add-to-cart, checkout funnel, and purchases with realistic user sessions and browsing behavior patterns.
- **Order events** — Order creation with items, pricing, discounts, shipping, payment info, and status lifecycle updates (pending → confirmed → processing → shipped → delivered / cancelled → refunded).
- **Inventory events** — Stock changes from sales, restocks, adjustments, and damaged goods across multiple warehouses.

The generator intentionally introduces **data quality issues** (null values, duplicates, late-arriving events, corrupted fields) to make the pipeline realistic.

#### `spark_streaming/`
Apache Spark Structured Streaming application that consumes events from Kafka topics and writes them to Delta Lake in the bronze layer. Each job:
1. Reads from a Kafka topic as a streaming DataFrame.
2. Parses JSON messages using predefined PySpark schemas (`schemas/bronze.py`).
3. Writes to Delta Lake in append mode with 10-second processing triggers.
4. Runs Delta table optimization (compaction and vacuum) on startup.

#### `dbt_project/`
A dbt project that transforms raw bronze data into analytics-ready models via Spark Thrift Server. The model layers follow the dbt best-practice architecture:
- **`source/`** — Reads raw Delta Lake tables from the bronze layer.
- **`staging/`** — Light cleaning and column standardization.
- **`intermediate/`** — Business logic, joins, and aggregations.
- **`marts/`** — Final analytics-ready tables (e.g., `dim_events_by_traffic` for traffic source analysis).

All models are materialized as Delta Lake tables.

#### `data/`
Runtime data directory where Spark writes Delta Lake tables and streaming checkpoints. The `bronze/` subdirectory contains three Delta tables (clickstream, orders, inventory). The `checkpoints/` subdirectory stores Spark Structured Streaming offsets for exactly-once processing guarantees.

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (v20.10+)
- [Docker Compose](https://docs.docker.com/compose/install/) (v2.0+)
- At least **4 GB RAM** and **2 CPUs** allocated to Docker
- At least **10 GB** free disk space

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/dhainiksuthar/Ecommerce-Analytics.git
   cd Ecommerce-Analytics
   ```

2. **Build the Spark image**
   
   The Spark image must be built separately before starting the stack, as Airflow uses it via `DockerOperator`:
   ```bash
   docker compose build spark
   ```

3. **Start all services**
   ```bash
   docker compose up -d
   ```
   This will start Kafka, Kafka UI, Spark Thrift Server, dbt, Metabase, Airflow (API server, scheduler, DAG processor, worker, triggerer), Redis, PostgreSQL, and the event producer.

4. **Wait for Airflow initialization**
   
   The `airflow-init` service will run first to set up the database and create the admin user. Monitor it with:
   ```bash
   docker compose logs -f airflow-init
   ```

5. **Access the services**

   | Service | URL | Credentials |
   |---|---|---|
   | Airflow UI | [http://localhost:8080](http://localhost:8080) | `airflow` / `airflow` |
   | Kafka UI | [http://localhost:8083](http://localhost:8083) | — |
   | Metabase | [http://localhost:3000](http://localhost:3000) | Set up on first visit |
   | Spark UI | [http://localhost:4043](http://localhost:4043) | — |
   | Flower (Celery) | [http://localhost:5555](http://localhost:5555) | Start with `--profile flower` |

6. **Trigger the Spark Streaming DAG**

   In the Airflow UI, unpause and trigger the `ecommerce_spark_streaming` DAG. This will launch three Spark Streaming containers (clickstream, orders, inventory) that continuously consume from Kafka and write to Delta Lake.

7. **Run dbt transformations**

   The `ecommerce_analytics_dbt` DAG runs automatically on schedule (twice daily). You can also trigger it manually from the Airflow UI, or run dbt commands directly:
   ```bash
   docker compose exec dbt dbt run
   docker compose exec dbt dbt test
   ```

### Stopping the Stack

```bash
docker compose down
```

To also remove all data volumes:
```bash
docker compose down -v
```

## 📨 Kafka Topics

| Topic | Description | Key |
|---|---|---|
| `clickstream` | User browsing events (page views, searches, cart actions, purchases) | `session_id` |
| `orders` | Order lifecycle events (creation, status updates, cancellations, refunds) | `order_id` |
| `inventory` | Stock changes (sales, restocks, adjustments, damaged goods) | `product_id` |

## 📦 Docker Services

| Service | Image | Description |
|---|---|---|
| `event-producer` | Custom (Dockerfile) | Generates and publishes e-commerce events to Kafka |
| `kafka` | `apache/kafka-native` | Message broker (KRaft mode, single node) |
| `kafka-ui` | `kafbat/kafka-ui` | Web UI for Kafka monitoring (port 8083) |
| `spark` | Custom (Dockerfile.spark) | Build-only image used by Airflow's DockerOperator |
| `spark-thrift` | Custom (Dockerfile.spark) | Spark Thrift Server for JDBC/SQL access to Delta tables |
| `dbt` | `ghcr.io/dbt-labs/dbt-spark:1.9.0` | dbt container for running transformations |
| `metabase` | `metabase/metabase` | Analytics dashboards and visualization (port 3000) |
| `postgres-metabase` | `postgres:16` | Metabase application database |
| `postgres-airflow` | `postgres:16` | Airflow metadata database |
| `redis` | `redis:7.2-bookworm` | Celery broker for Airflow |
| `airflow-apiserver` | Custom (Dockerfile) | Airflow API server and web UI (port 8080) |
| `airflow-scheduler` | Custom (Dockerfile) | Airflow scheduler |
| `airflow-dag-processor` | Custom (Dockerfile) | Airflow DAG processor |
| `airflow-worker` | Custom (Dockerfile) | Airflow Celery worker |
| `airflow-triggerer` | Custom (Dockerfile) | Airflow triggerer for deferred tasks |
| `airflow-init` | Custom (Dockerfile) | One-shot initialization (DB migration, admin user creation) |
| `airflow-cli` | Custom (Dockerfile) | Debug profile for running Airflow CLI commands |
| `flower` | Custom (Dockerfile) | Celery Flower monitoring (port 5555, optional profile) |

## 🔧 Configuration

Key environment variables in `.env`:

| Variable | Default | Description |
|---|---|---|
| `AIRFLOW_UID` | `50000` | User ID for Airflow containers |
| `AIRFLOW_IMAGE_NAME` | `apache/airflow:2.10.0` | Base Airflow Docker image |
| `AIRFLOW_PROJ_DIR` | `.` | Project root directory |

## 👤 Author

**Dhainik Suthar**  
GitHub: [@dhainiksuthar](https://github.com/dhainiksuthar)
