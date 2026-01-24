from pyspark.sql.types import StructType, StructField, StringType, DoubleType

clickstream_brz_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_path", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])