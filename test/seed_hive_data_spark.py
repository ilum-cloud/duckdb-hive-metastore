#!/usr/bin/env python3
"""
PySpark script to seed sample data into Hive Metastore with different storage formats.

This script creates sample tables in different formats:
- Parquet (default format)
- Delta Lake
- CSV

All data is stored in S3 (RustFS) and registered in the Hive Metastore.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    ShortType,
    ByteType,
    BooleanType,
    FloatType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
    BinaryType,
)
from datetime import datetime, timedelta
from decimal import Decimal


def create_spark_session():
    """Create and configure Spark session with Hive and S3 support."""

    spark = (
        SparkSession.builder.appName("Hive Metastore Data Seeder")
        .config("spark.sql.warehouse.dir", "s3a://test-bucket/")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
        .config("spark.hadoop.fs.s3a.etag.checksum.enabled", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
        )
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.hive.convertMetastoreParquet", "true")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hive")
        .config("spark.sql.catalog.local.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.local.warehouse", "s3a://test-bucket/")
        .config("spark.ui.showConsoleProgress", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def create_sample_customers_data(spark, num_rows=100):
    """Create sample customer data."""

    first_names = ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Diana', 'Eve', 'Frank']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Spain', 'Italy', 'Japan']

    data = []
    for i in range(1, num_rows + 1):
        customer_id = i
        first_name = first_names[(i - 1) % len(first_names)]
        last_name = last_names[(i - 1) % len(last_names)]
        email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
        country = countries[(i - 1) % len(countries)]
        # Increment date by 10 days for each customer
        signup_date = (datetime(2020, 1, 1) + timedelta(days=(i - 1) * 10)).date()

        data.append((customer_id, first_name, last_name, email, country, signup_date))

    schema = StructType(
        [
            StructField("customer_id", IntegerType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
            StructField("signup_date", DateType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_products_data(spark, num_rows=50):
    """Create sample product data."""

    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys']
    data = []

    for i in range(1, num_rows + 1):
        product_id = i
        product_name = f"Product {i}"
        category = categories[(i - 1) % len(categories)]
        # Price increases with product_id: 10.00, 20.00, 30.00, etc.
        price = round(10.0 * i, 2)
        # Stock quantity: 100, 200, 300, etc.
        stock_quantity = i * 100

        data.append((product_id, product_name, category, price, stock_quantity))

    schema = StructType(
        [
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("stock_quantity", IntegerType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_orders_data(spark, num_rows=200):
    """Create sample orders data."""

    statuses = ['pending', 'shipped', 'delivered', 'cancelled']
    data = []

    for i in range(1, num_rows + 1):
        order_id = i
        # Cycle through customers (1-100)
        customer_id = ((i - 1) % 100) + 1
        # Cycle through products (1-50)
        product_id = ((i - 1) % 50) + 1
        # Quantity: 1-10 repeating
        quantity = ((i - 1) % 10) + 1
        # Total amount based on quantity and product_id
        total_amount = round(quantity * product_id * 10.0, 2)
        # Order date increments by 5 days for each order
        order_date = (datetime(2020, 1, 1) + timedelta(days=(i - 1) * 5)).date()
        # Cycle through statuses
        status = statuses[(i - 1) % len(statuses)]

        data.append((order_id, customer_id, product_id, quantity, total_amount, order_date, status))

    schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("order_date", DateType(), True),
            StructField("status", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_reviews_data(spark, num_rows=150):
    """Create sample product reviews data."""

    review_texts = [
        "Great product!",
        "Not bad, could be better.",
        "Excellent quality!",
        "Would not recommend.",
        "Amazing value for money!",
    ]

    data = []

    for i in range(1, num_rows + 1):
        review_id = i
        # Cycle through products (1-50)
        product_id = ((i - 1) % 50) + 1
        # Cycle through customers (1-100)
        customer_id = ((i - 1) % 100) + 1
        # Cycle through ratings 1-5
        rating = ((i - 1) % 5) + 1
        # Select review text based on index
        review_text = f"This is a sample review {i}. " + review_texts[(i - 1) % len(review_texts)]
        # Review date increments by 7 days for each review
        review_date = (datetime(2020, 1, 1) + timedelta(days=(i - 1) * 7)).date()

        data.append((review_id, product_id, customer_id, rating, review_text, review_date))

    schema = StructType(
        [
            StructField("review_id", IntegerType(), False),
            StructField("product_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("rating", IntegerType(), True),
            StructField("review_text", StringType(), True),
            StructField("review_date", DateType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_inventory_data(spark, num_rows=75):
    """Create sample inventory data for Iceberg table."""

    locations = ['Warehouse A', 'Warehouse B', 'Warehouse C', 'Store 1', 'Store 2']
    data = []

    for i in range(1, num_rows + 1):
        inventory_id = i
        product_id = ((i - 1) % 50) + 1
        location = locations[(i - 1) % len(locations)]
        quantity = i * 10
        last_updated = (datetime(2024, 1, 1) + timedelta(days=(i - 1) * 5)).date()

        data.append((inventory_id, product_id, location, quantity, last_updated))

    schema = StructType(
        [
            StructField("inventory_id", IntegerType(), False),
            StructField("product_id", IntegerType(), True),
            StructField("location", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("last_updated", DateType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_shipments_data(spark, num_rows=80):
    """Create sample shipment data for Avro table."""

    carriers = ['FedEx', 'UPS', 'DHL', 'USPS']
    statuses = ['in_transit', 'delivered', 'delayed']
    data = []

    for i in range(1, num_rows + 1):
        shipment_id = i
        order_id = ((i - 1) % 200) + 1
        carrier = carriers[(i - 1) % len(carriers)]
        tracking_number = f"TRK{i:08d}"
        status = statuses[(i - 1) % len(statuses)]
        ship_date = (datetime(2024, 1, 1) + timedelta(days=(i - 1) * 3)).date()

        data.append((shipment_id, order_id, carrier, tracking_number, status, ship_date))

    schema = StructType(
        [
            StructField("shipment_id", IntegerType(), False),
            StructField("order_id", IntegerType(), True),
            StructField("carrier", StringType(), True),
            StructField("tracking_number", StringType(), True),
            StructField("status", StringType(), True),
            StructField("ship_date", DateType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_type_test_data(spark, num_rows=100):
    """Create sample data with diverse types for type mapping testing."""

    data = []
    for i in range(1, num_rows + 1):
        test_id = i
        tiny_int_col = i % 128  # TINYINT: -128 to 127
        small_int_col = i % 32768  # SMALLINT: -32768 to 32767
        big_int_col = i * 1000000000  # BIGINT: large integer
        float_col = round(1.5 * i, 2)  # FLOAT: 32-bit floating point
        double_col = round(3.14159 * i, 6)  # DOUBLE: 64-bit floating point
        decimal_col = Decimal(str(round(99.99 + (i * 0.01), 2)))  # DECIMAL(10,2): precise decimal
        is_active = i % 2 == 0  # BOOLEAN: true/false
        timestamp_col = datetime(2020, 1, 1) + timedelta(hours=i)
        binary_col = bytes([i % 256, (i + 1) % 256, (i + 2) % 256])  # BINARY: raw bytes
        category = ['A', 'B', 'C'][(i - 1) % 3]  # STRING/VARCHAR

        data.append(
            (
                test_id,
                tiny_int_col,
                small_int_col,
                big_int_col,
                float_col,
                double_col,
                decimal_col,
                is_active,
                timestamp_col,
                binary_col,
                category,
            )
        )

    schema = StructType(
        [
            StructField("test_id", IntegerType(), False),
            StructField("tiny_int_col", ByteType(), True),
            StructField("small_int_col", ShortType(), True),
            StructField("big_int_col", LongType(), True),
            StructField("float_col", FloatType(), True),
            StructField("double_col", DoubleType(), True),
            StructField("decimal_col", DecimalType(10, 2), True),
            StructField("is_active", BooleanType(), True),
            StructField("timestamp_col", TimestampType(), True),
            StructField("binary_col", BinaryType(), True),
            StructField("category", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_decimal_partitioned_data(spark, num_rows=50):
    """Create sample data partitioned by DECIMAL type (problematic for type mapping)."""

    regions = ['North', 'South', 'East', 'West']
    data = []

    for i in range(1, num_rows + 1):
        transaction_id = i
        amount = Decimal(str(round(10.0 + (i % 100) * 0.99, 2)))  # DECIMAL(10,2)
        region = regions[(i - 1) % len(regions)]
        transaction_date = (datetime(2020, 1, 1) + timedelta(days=i)).date()
        description = f"Transaction {i}"

        data.append((transaction_id, amount, region, transaction_date, description))

    schema = StructType(
        [
            StructField("transaction_id", IntegerType(), False),
            StructField("amount", DecimalType(10, 2), True),
            StructField("region", StringType(), True),
            StructField("transaction_date", DateType(), True),
            StructField("description", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def create_sample_multi_partition_data(spark, num_rows=60):
    """Create sample data partitioned by multiple columns (year, month, region)."""

    regions = ['North', 'South', 'East', 'West']
    statuses = ['pending', 'completed', 'cancelled']
    data = []

    for i in range(1, num_rows + 1):
        event_id = i
        year = 2020 + ((i - 1) // 12)  # Multiple years
        month = ((i - 1) % 12) + 1  # Months 1-12
        region = regions[(i - 1) % len(regions)]
        status = statuses[(i - 1) % len(statuses)]
        value = round(100.0 * i, 2)
        event_timestamp = datetime(year, month, 1, 12, 0, 0)

        data.append((event_id, year, month, region, status, value, event_timestamp))

    schema = StructType(
        [
            StructField("event_id", IntegerType(), False),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("region", StringType(), True),
            StructField("status", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("event_timestamp", TimestampType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def main():
    """Main function to seed data into Hive Metastore."""

    print("=" * 80)
    print("Starting Hive Metastore Data Seeding Process")
    print("=" * 80)

    # Create Spark session
    print("\n[1/11] Creating Spark session with Hive and S3 support...")
    spark = create_spark_session()
    print("✓ Spark session created successfully")

    # Create database
    print("\n[2/11] Creating database 'sample_db'...")
    spark.sql("CREATE DATABASE IF NOT EXISTS sample_db")
    spark.sql("USE sample_db")
    print("✓ Database 'sample_db' created and selected")

    # Create and save Customers table (Parquet - default format)
    print("\n[3/11] Creating 'customers' table in Parquet format...")
    customers_df = create_sample_customers_data(spark, num_rows=100)
    customers_df.write.mode("overwrite").format("parquet").saveAsTable("sample_db.customers")
    print(f"✓ Created 'customers' table with {customers_df.count()} rows in Parquet format")
    print(f"   Location: s3a://test-bucket/sample_db.db/customers/")

    # Create and save Products table (Delta format)
    print("\n[4/11] Creating 'products' table in Delta format...")
    products_df = create_sample_products_data(spark, num_rows=50)

    # Drop table if exists to avoid truncate mode issues with Delta
    spark.sql("DROP TABLE IF EXISTS sample_db.products")

    # Write as Delta table
    delta_path = "s3a://test-bucket/sample_db.db/products/"
    products_df.write.format("delta").mode("overwrite").save(delta_path)

    # Create table in metastore pointing to Delta location
    spark.sql(
        f"""
        CREATE TABLE sample_db.products
        USING delta
        LOCATION '{delta_path}'
    """
    )
    print(f"✓ Created 'products' table with {products_df.count()} rows in Delta format")
    print(f"   Location: {delta_path}")

    # Create and save Orders table (CSV format)
    print("\n[5/11] Creating 'orders' table in CSV format...")
    orders_df = create_sample_orders_data(spark, num_rows=200)

    # First save as CSV to S3
    csv_path = "s3a://test-bucket/sample_db.db/orders_csv/"
    orders_df.write.mode("overwrite").format("csv").option("header", "true").save(csv_path)

    # Then create external table pointing to CSV files
    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.orders (
            order_id INT,
            customer_id INT,
            product_id INT,
            quantity INT,
            total_amount DOUBLE,
            order_date DATE,
            status STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{csv_path}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """
    )
    print(f"✓ Created 'orders' table with {orders_df.count()} rows in CSV format")
    print(f"   Location: {csv_path}")

    # Create and save Reviews table (Parquet with partitioning)
    print("\n[6/11] Creating 'reviews' table in Parquet format with partitioning...")
    reviews_df = create_sample_reviews_data(spark, num_rows=150)
    reviews_df.write.mode("overwrite").format("parquet").partitionBy("rating").saveAsTable("sample_db.reviews")
    print(f"✓ Created 'reviews' table with {reviews_df.count()} rows in Parquet format (partitioned by rating)")
    print(f"   Location: s3a://test-bucket/sample_db.db/reviews/")

    # Create and save Inventory table (Iceberg format) - if Iceberg is available
    print("\n[7/11] Creating 'inventory' table in Iceberg format...")
    try:
        inventory_df = create_sample_inventory_data(spark, num_rows=75)

        # Drop table if exists
        spark.sql("DROP TABLE IF EXISTS local.sample_db.inventory")

        # Create Iceberg table using Iceberg catalog with HMS backend
        iceberg_path = "s3a://test-bucket/sample_db.db/inventory_iceberg/"
        inventory_df.writeTo("local.sample_db.inventory").using("iceberg").tableProperty(
            "location", iceberg_path
        ).createOrReplace()

        # Verify table was created and set table_type parameter for HMS detection
        spark.sql("DESCRIBE EXTENDED local.sample_db.inventory").show(100, truncate=False)

        print(f"✓ Created 'inventory' table with {inventory_df.count()} rows in Iceberg format")
        print(f"   Location: {iceberg_path}")
        print(f"   Table registered in HMS with table_type=ICEBERG")
    except Exception as e:
        print(f"⚠ Skipped 'inventory' table (Iceberg): {str(e)[:200]}")
        import traceback

        traceback.print_exc()

    # Create and save Shipments table (Avro format)
    print("\n[8/11] Creating 'shipments' table in Avro format...")
    try:
        shipments_df = create_sample_shipments_data(spark, num_rows=80)

        # Save as Avro to S3
        avro_path = "s3a://test-bucket/sample_db.db/shipments_avro/"
        shipments_df.write.mode("overwrite").format("avro").save(avro_path)

        # Create external table pointing to Avro files
        spark.sql(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS sample_db.shipments (
                shipment_id INT,
                order_id INT,
                carrier STRING,
                tracking_number STRING,
                status STRING,
                ship_date DATE
            )
            STORED AS AVRO
            LOCATION '{avro_path}'
        """
        )
        print(f"✓ Created 'shipments' table with {shipments_df.count()} rows in Avro format")
        print(f"   Location: {avro_path}")
    except Exception as e:
        print(f"⚠ Skipped 'shipments' table (Avro): {str(e)[:100]}")

    # Create and save type_test table (Parquet with diverse types)
    print("\n[9/11] Creating 'type_test' table in Parquet format with diverse types...")
    type_test_df = create_sample_type_test_data(spark, num_rows=100)
    type_test_df.write.mode("overwrite").format("parquet").saveAsTable("sample_db.type_test")
    print(f"✓ Created 'type_test' table with {type_test_df.count()} rows in Parquet format")
    print(f"   Location: s3a://test-bucket/sample_db.db/type_test/")
    print("   Types: TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL(10,2),")
    print("          BOOLEAN, TIMESTAMP, BINARY, VARCHAR")

    # Create and save decimal_partitioned table (Parquet partitioned by DECIMAL)
    print("\n[10/11] Creating 'decimal_partitioned' table in Parquet format (partitioned by DECIMAL)...")
    decimal_partitioned_df = create_sample_decimal_partitioned_data(spark, num_rows=50)
    decimal_partitioned_df.write.mode("overwrite").format("parquet").partitionBy("amount").saveAsTable(
        "sample_db.decimal_partitioned"
    )
    print(f"✓ Created 'decimal_partitioned' table with {decimal_partitioned_df.count()} rows in Parquet format")
    print(f"   Location: s3a://test-bucket/sample_db.db/decimal_partitioned/")
    print("   Partitioned by: amount (DECIMAL(10,2)) - tests problematic partition type")

    # Create and save multi_partition table (Parquet partitioned by multiple columns)
    print("\n[11/11] Creating 'multi_partition' table in Parquet format (partitioned by year, month, region)...")
    multi_partition_df = create_sample_multi_partition_data(spark, num_rows=60)
    multi_partition_df.write.mode("overwrite").format("parquet").partitionBy("year", "month", "region").saveAsTable(
        "sample_db.multi_partition"
    )
    print(f"✓ Created 'multi_partition' table with {multi_partition_df.count()} rows in Parquet format")
    print(f"   Location: s3a://test-bucket/sample_db.db/multi_partition/")
    print("   Partitioned by: year (INTEGER), month (INTEGER), region (VARCHAR)")

    # Display summary
    print("\n" + "=" * 80)
    print("Data Seeding Complete - Summary")
    print("=" * 80)
    print("\nTables created in 'sample_db' database:")
    print()
    print("┌─────────────────────┬────────┬───────────┬─────────────────────────────────────────┐")
    print("│ Table               │ Format │ Rows      │ Notes                                   │")
    print("├─────────────────────┼────────┼───────────┼─────────────────────────────────────────┤")
    print("│ customers           │ Parquet│ 100       │ Basic types (INT, VARCHAR, DATE)        │")
    print("│ products            │ Delta  │ 50        │ Product catalog                          │")
    print("│ orders              │ CSV    │ 200       │ External table with header               │")
    print("│ reviews             │ Parquet│ 150       │ Partitioned by rating (INT)              │")
    print("│ inventory           │ Iceberg│ 75        │ (if Iceberg available)                   │")
    print("│ shipments           │ Avro   │ 80        │ (if Avro available)                      │")
    print("│ type_test           │ Parquet│ 100       │ DIVERSE TYPES - full type mapping test   │")
    print("│ decimal_partitioned │ Parquet│ 50        │ Partitioned by amount (DECIMAL(10,2))    │")
    print("│ multi_partition     │ Parquet│ 60        │ Partitioned by year, month, region       │")
    print("└─────────────────────┴────────┴───────────┴─────────────────────────────────────────┘")
    print()
    print("Type Testing Coverage:")
    print("  - TINYINT, SMALLINT, INTEGER, BIGINT")
    print("  - FLOAT, DOUBLE, DECIMAL(10,2)")
    print("  - BOOLEAN, DATE, TIMESTAMP")
    print("  - BINARY, VARCHAR")
    print("  - Partitioned by INT (reviews)")
    print("  - Partitioned by DECIMAL (decimal_partitioned) - problematic type")
    print("  - Multi-column partitioning (multi_partition)")
    print()

    # Show some sample queries
    print("\nSample queries to verify data:")
    print("-" * 80)

    print("\n1. Show all databases:")
    spark.sql("SHOW DATABASES").show()

    print("\n2. Show tables in sample_db:")
    spark.sql("SHOW TABLES IN sample_db").show()

    print("\n3. Sample data from customers (Parquet):")
    spark.sql("SELECT * FROM sample_db.customers LIMIT 5").show()

    print("\n4. Sample data from products (Delta):")
    spark.sql("SELECT * FROM sample_db.products LIMIT 5").show()

    print("\n5. Sample data from orders (CSV):")
    spark.sql("SELECT * FROM sample_db.orders LIMIT 5").show()

    print("\n6. Sample data from reviews (Parquet, partitioned):")
    spark.sql("SELECT * FROM sample_db.reviews LIMIT 5").show()

    print("\n7. Table statistics:")
    print("\nCustomers count by country:")
    spark.sql("SELECT country, COUNT(*) as count FROM sample_db.customers GROUP BY country ORDER BY count DESC").show()

    print("\nProducts count by category:")
    spark.sql("SELECT category, COUNT(*) as count FROM sample_db.products GROUP BY category ORDER BY count DESC").show()

    print("\nOrders count by status:")
    spark.sql("SELECT status, COUNT(*) as count FROM sample_db.orders GROUP BY status ORDER BY count DESC").show()

    print("\nReviews count by rating:")
    spark.sql("SELECT rating, COUNT(*) as count FROM sample_db.reviews GROUP BY rating ORDER BY rating").show()

    print("\n8. Type testing - diverse types:")
    print("\nDescribe type_test table (showing all column types):")
    spark.sql("DESCRIBE sample_db.type_test").show(100, truncate=False)

    print("\nSample data from type_test:")
    spark.sql("SELECT * FROM sample_db.type_test LIMIT 5").show(truncate=False)

    print("\nAggregation tests on type_test:")
    print("  - SUM of decimal_col (DECIMAL(10,2)):")
    spark.sql("SELECT SUM(decimal_col) as total FROM sample_db.type_test").show()

    print("  - AVG of float_col (FLOAT):")
    spark.sql("SELECT AVG(float_col) as avg_float FROM sample_db.type_test").show()

    print("  - Filter by boolean column:")
    spark.sql("SELECT COUNT(*) as active_count FROM sample_db.type_test WHERE is_active = true").show()

    print("\n9. Decimal partitioned table:")
    print("\nDescribe decimal_partitioned table:")
    spark.sql("DESCRIBE sample_db.decimal_partitioned").show(100, truncate=False)

    print("\nSample data from decimal_partitioned:")
    spark.sql("SELECT * FROM sample_db.decimal_partitioned ORDER BY amount LIMIT 10").show(truncate=False)

    print("\nPartition values (amount should be DECIMAL(10,2), not BIGINT):")
    spark.sql("SELECT DISTINCT amount FROM sample_db.decimal_partitioned ORDER BY amount").show()

    print("\n10. Multi-partitioned table:")
    print("\nDescribe multi_partition table:")
    spark.sql("DESCRIBE sample_db.multi_partition").show(100, truncate=False)

    print("\nSample data from multi_partition:")
    spark.sql("SELECT * FROM sample_db.multi_partition ORDER BY year, month, region LIMIT 10").show(truncate=False)

    print("\nPartition combinations (year, month, region):")
    spark.sql(
        "SELECT year, month, region, COUNT(*) as count FROM sample_db.multi_partition GROUP BY year, month, region ORDER BY year, month, region"
    ).show()

    print("\n" + "=" * 80)
    print("Data seeding completed successfully!")
    print("=" * 80)

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()
