from spark_streaming.utils.spark_session import spark
from spark_streaming.schemas.clickstream_to_bronze import clickstream_brz_schema
from pyspark.sql.functions import from_json, col
import psycopg2

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

def _write_batch_(batch_df, batch_id, DB_CONFIG):
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id} is empty")
        return
    
    row_count = batch_df.count()
    logger.info(f"Batch {batch_id}: Writing {row_count} rows")
    batch_df.write \
        .format("jdbc") \
        .option("url", DB_CONFIG['url']) \
        .option("dbtable", DB_CONFIG['table']) \
        .option("user", DB_CONFIG['user']) \
        .option("password", DB_CONFIG['password']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def writeStream(table_name, dfBronze, DB_CONFIG):
    return dfBronze.writeStream \
            .foreachBatch(lambda df, id: _write_batch_(df, id, DB_CONFIG)) \
            .outputMode("append") \
            .start()

if __name__ == "__main__":
    topic_name = "ClickStream"
    table_name = "clickstream"
    database_name = "metabase_db"
    DB_CONFIG = {
        "url": f"jdbc:postgresql://postgres:5432/{database_name}",
        "table": "kafka_streams",
        "user": "metabase",
        "password": "mysecretpassword"
    }
    cursor = postgresCon(DB_CONFIG)
    createDatabase(database_name, cursor)
    cursor.close()
    # createTable(database_name, table_name, cursor)
    dfBronze = readStream(topic_name)
    query = writeStream(table_name, dfBronze, DB_CONFIG)
    query.awaitTermination()
