from spark_streaming.utils.spark_session import spark
from spark_streaming.schemas.bronze import inventory_brz_schema
from pyspark.sql.functions import from_json, col
from delta.tables import DeltaTable

spark.sparkContext.setLogLevel("WARN")

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def readStream(topic_name):
    InventoryBrzRawdf = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9093") \
        .option("subscribe", topic_name) \
        .load()

    InventoryBrzdf = InventoryBrzRawdf.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), inventory_brz_schema).alias("data")) \
    .select("data.*")
    logging.info(InventoryBrzdf)

    return InventoryBrzdf

def optimize_table(path):
    if DeltaTable.isDeltaTable(spark, path):    
        print(f"Optimizing {path}")
        deltaTable = DeltaTable.forPath(spark, path)
        deltaTable.optimize().executeCompaction()
        deltaTable.vacuum()
    else:
        print("Tables Does not exists, Skipping")

def writeStream(path, checkpoint_path, dfBronze):
    return dfBronze.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="10 seconds") \
        .start(path)

if __name__ == "__main__":
    topic_name = "inventory"
    path = "data/bronze/inventory"
    checkpoint_path = "data/checkpoints/inventory"

    optimize_table(path)
    print(f"Reading Started {topic_name}")
    dfBronze = readStream(topic_name)
    print(f"Writing Started {topic_name}")
    query = writeStream(path, checkpoint_path, dfBronze)
    query.awaitTermination()
