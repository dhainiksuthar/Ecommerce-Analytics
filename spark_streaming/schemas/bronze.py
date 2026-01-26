from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType, DateType, ArrayType, DecimalType

clickstream_brz_schema = StructType([

    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("event_date", DateType(), True),
    StructField("event_hour", IntegerType(), True),
    StructField("is_late_arriving", BooleanType(), True),

    StructField("user_id", StringType(), True),
    StructField("is_anonymous", BooleanType(), True),
    StructField("loyalty_tier", StringType(), True),

    StructField("session_id", StringType(), True),

    StructField("device_type", StringType(), True),
    StructField("device_os", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("browser_version", StringType(), True),
    StructField("screen_resolution", StringType(), True),

    StructField("traffic_source", StringType(), True),
    StructField("traffic_medium", StringType(), True),
    StructField("campaign", StringType(), True),

    StructField("country_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ip_address", StringType(), True),

    StructField("page_url", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("referrer_url", StringType(), True),

    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_subcategory", StringType(), True),
    StructField("product_brand", StringType(), True),
    StructField("product_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),

    StructField("search_query", StringType(), True),
    StructField("search_results_count", IntegerType(), True),

    StructField("cart_id", StringType(), True),
    StructField("cart_total", DoubleType(), True),
    StructField("cart_item_count", IntegerType(), True),

    StructField("order_id", StringType(), True),

    StructField("time_on_page_seconds", IntegerType(), True),
    StructField("scroll_depth_percent", IntegerType(), True),
    StructField("click_count", IntegerType(), True),

    StructField("event_sequence", IntegerType(), True),
    StructField("is_entrance", BooleanType(), True),
    StructField("is_exit", BooleanType(), True),

    StructField("_data_quality_score", DoubleType(), True),
    StructField("_processing_timestamp", TimestampType(), True)

])

inventory_brz_schema = StructType([
    StructField("event_id", StringType(), False),

    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("is_late_arriving", BooleanType(), True),

    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),

    StructField("quantity_change", IntegerType(), True),
    StructField("quantity_before", IntegerType(), True),
    StructField("quantity_after", IntegerType(), True),
    StructField("reserved_quantity", IntegerType(), True),

    StructField("reason", StringType(), True),

    StructField("warehouse_id", StringType(), True),
    StructField("warehouse_location", StringType(), True),
    StructField("supplier_id", StringType(), True)
])

order_brz_schema = StructType([

    # event metadata
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("is_late_arriving", BooleanType(), True),

    # order identifiers & timing
    StructField("order_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_date", DateType(), True),        # "2026-01-26"
    StructField("order_time", StringType(), True),      # "07:03:48" (kept as string)

    # user / customer
    StructField("user_id", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_first_name", StringType(), True),
    StructField("customer_last_name", StringType(), True),
    StructField("customer_phone", StringType(), True),
    StructField("is_guest_checkout", BooleanType(), True),
    StructField("loyalty_tier", StringType(), True),

    # session / device / traffic
    StructField("session_id", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("traffic_source", StringType(), True),

    # shipping / billing
    StructField("shipping_country", StringType(), True),
    StructField("shipping_city", StringType(), True),
    StructField("shipping_postal_code", StringType(), True),
    StructField("shipping_address_line1", StringType(), True),
    StructField("shipping_address_line2", StringType(), True),
    StructField("billing_same_as_shipping", BooleanType(), True),

    # items (array of structs)
    StructField("items", ArrayType(
        StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("price", DecimalType(10, 2), True),   # use Decimal for money
            StructField("quantity", IntegerType(), True),
        ])
    ), True),

    # order totals & counts
    StructField("item_count", IntegerType(), True),
    StructField("unique_item_count", IntegerType(), True),
    StructField("subtotal", DecimalType(12, 2), True),
    StructField("discount_amount", DecimalType(12, 2), True),
    StructField("coupon_code", StringType(), True),
    StructField("shipping_cost", DecimalType(10, 2), True),
    StructField("shipping_method", StringType(), True),
    StructField("estimated_delivery_days", IntegerType(), True),

    # tax & totals
    StructField("tax_amount", DecimalType(12, 2), True),
    StructField("tax_rate", DecimalType(5, 4), True),   # e.g. 0.08
    StructField("total_amount", DecimalType(12, 2), True),
    StructField("currency", StringType(), True),

    # payment
    StructField("payment_method", StringType(), True),
    StructField("payment_provider", StringType(), True),
    StructField("payment_status", StringType(), True),

    # record timestamps
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("shipped_at", TimestampType(), True),
    StructField("delivered_at", TimestampType(), True),

    # fulfillment / extras
    StructField("warehouse_id", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("carrier", StringType(), True),

    StructField("is_first_order", BooleanType(), True),
    StructField("is_gift", BooleanType(), True),
    StructField("gift_message", StringType(), True),

    # quality score
    StructField("_data_quality_score", DecimalType(4, 2), True)
])