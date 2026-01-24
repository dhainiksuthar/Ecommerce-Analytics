from spark_streaming.utils.spark_session import spark
from spark_streaming.schemas.clickstream_to_bronze import clickstream_brz_schema
from pyspark.sql.functions import from_json, col

spark.sparkContext.setLogLevel("WARN")

ClickBronzeRawdf = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", "ClickStream") \
    .load()

ClickBronzedf = ClickBronzeRawdf.selectExpr("CAST(value AS STRING)") \
.select(from_json(col("value"), clickstream_brz_schema).alias("data")) \
.select("data.*")

query = ClickBronzedf.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='10 seconds') \
    .outputMode("append") \
    .start()

query.awaitTermination()
