from spark_streaming.utils.spark_session import spark
from spark_streaming.schemas.clickstream_to_bronze import clickstream_brz_schema
from pyspark.sql.functions import from_json, col
import psycopg2
from delta.tables import DeltaTable

spark.sparkContext.setLogLevel("WARN")

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def postgresCon(DB_CONFIG):
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database="postgres"
    )
    conn.autocommit = True
    cursor = conn.cursor()
    return cursor

def createDatabase(database_name, cursor):
    query = f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"
    cursor.execute(query)
    if not cursor.fetchone():
        cursor.execute(f"CREATE DATABASE {database_name}")
    return

# def createTable(database_name, table_name, cursor):
#     query = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name}"
#     cursor.execute(query)
#     return

def readStream(topic_name):
    ClickBronzeRawdf = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9093") \
        .option("subscribe", topic_name) \
        .load()

    ClickBronzedf = ClickBronzeRawdf.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), clickstream_brz_schema).alias("data")) \
    .select("data.*")

    return ClickBronzedf

def optimize_table(path):
    print(f"Optimizing {path}")
    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.optimize().executeCompaction()
    deltaTable.vacuum()

def writeStream(path, dfBronze):
    return dfBronze.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "data/checkpoints/clickstream") \
        .trigger(processingTime="10 seconds") \
        .start(path)

if __name__ == "__main__":
    topic_name = "ClickStream"
    path = "data/bronze/clickstream"
    database_name = "metabase_db"

    optimize_table(path)
    print("Reading Started")
    dfBronze = readStream(topic_name)
    print("Writing Started")
    query = writeStream(path, dfBronze)
    query.awaitTermination()
