from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit"
    " --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    " --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

with DAG(
    dag_id="ecommerce_spark_streaming",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Trigger once manually from the UI — runs indefinitely
    catchup=False,
    tags=["spark", "streaming", "bronze"],
):
    common = dict(
        image="ecommerce-spark:latest",  # built via: docker compose build spark
        network_mode="ecommerce-analytics_default",  # docker-compose project network
        auto_remove="force",
        mount_tmp_dir=False,
        mounts=[
            # Named volume defined in docker-compose.yaml — safe to reference from sibling containers
            Mount(source="ecommerce-analytics_spark-data", target="/opt/spark/work-dir/data", type="volume"),
        ],
    )

    DockerOperator(
        task_id="clickstream_to_bronze",
        command=f"{SPARK_SUBMIT} /opt/spark/work-dir/spark_streaming/jobs/clickstream_to_bronze.py",
        **common,
    )

    DockerOperator(
        task_id="orders_to_bronze",
        command=f"{SPARK_SUBMIT} /opt/spark/work-dir/spark_streaming/jobs/orders_to_bronze.py",
        **common,
    )

    DockerOperator(
        task_id="inventory_to_bronze",
        command=f"{SPARK_SUBMIT} /opt/spark/work-dir/spark_streaming/jobs/inventory_to_bronze.py",
        **common,
    )

    # All 3 tasks run in parallel — no dependencies between them
