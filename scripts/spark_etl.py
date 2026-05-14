import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType, LongType, NumericType, StringType, TimestampType


POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/bigdata")
POSTGRES_USER = os.getenv("POSTGRES_USER", "bigdata")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bigdata")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/bigdata")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("BigDataSparkLab2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_postgres_table(spark: SparkSession, table_name: str):
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_postgres_table(df, table_name: str) -> None:
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", POSTGRES_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .save()
    )


def write_clickhouse_table(df, table_name: str) -> None:
    df = prepare_for_clickhouse(df)
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", table_name)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
        .save()
    )


def prepare_for_clickhouse(df):
    fill_values = {}
    date_columns = []
    timestamp_columns = []

    for field in df.schema.fields:
        if not field.nullable:
            continue
        if isinstance(field.dataType, StringType):
            fill_values[field.name] = ""
        elif isinstance(field.dataType, NumericType):
            fill_values[field.name] = 0
        elif isinstance(field.dataType, BooleanType):
            fill_values[field.name] = False
        elif isinstance(field.dataType, DateType):
            date_columns.append(field.name)
        elif isinstance(field.dataType, TimestampType):
            timestamp_columns.append(field.name)

    if fill_values:
        df = df.fillna(fill_values)
    for column in date_columns:
        df = df.withColumn(column, F.coalesce(F.col(column), F.lit("1970-01-01").cast(DateType())))
    for column in timestamp_columns:
        df = df.withColumn(column, F.coalesce(F.col(column), F.lit("1970-01-01 00:00:00").cast(TimestampType())))

    return df


def typed_raw(raw):
    return (
        raw.withColumn("raw_id", F.col("raw_id").cast(LongType()))
        .withColumn("customer_age", F.col("customer_age").cast(IntegerType()))
        .withColumn("product_price", F.col("product_price").cast(DoubleType()))
        .withColumn("product_quantity", F.col("product_quantity").cast(IntegerType()))
        .withColumn("sale_quantity", F.col("sale_quantity").cast(IntegerType()))
        .withColumn("sale_total_price", F.col("sale_total_price").cast(DoubleType()))
        .withColumn("product_weight", F.col("product_weight").cast(DoubleType()))
        .withColumn("product_rating", F.col("product_rating").cast(DoubleType()))
        .withColumn("product_reviews", F.col("product_reviews").cast(IntegerType()))
        .withColumn("sale_date", F.to_date("sale_date", "M/d/yyyy"))
        .withColumn("product_release_date", F.to_date("product_release_date", "M/d/yyyy"))
        .withColumn("product_expiry_date", F.to_date("product_expiry_date", "M/d/yyyy"))
    )


def hash_key(*columns):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]), 256)


def build_dimensions(df):
    customer_cols = [
        "sale_customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
    ]
    dim_customer = (
        df.withColumn("customer_key", hash_key(*customer_cols))
        .select(
            "customer_key",
            F.col("sale_customer_id").alias("source_customer_id"),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
        )
        .dropDuplicates(["customer_key"])
    )

    seller_cols = [
        "sale_seller_id",
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code",
    ]
    dim_seller = (
        df.withColumn("seller_key", hash_key(*seller_cols))
        .select(
            "seller_key",
            F.col("sale_seller_id").alias("source_seller_id"),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
        )
        .dropDuplicates(["seller_key"])
    )

    product_cols = [
        "sale_product_id",
        "product_name",
        "product_category",
        "pet_category",
        "product_brand",
        "product_material",
        "product_color",
        "product_size",
    ]
    dim_product = (
        df.withColumn("product_key", hash_key(*product_cols))
        .select(
            "product_key",
            F.col("sale_product_id").alias("source_product_id"),
            "product_name",
            "product_category",
            "pet_category",
            "product_price",
            "product_quantity",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
        )
        .dropDuplicates(["product_key"])
    )

    store_cols = [
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ]
    dim_store = (
        df.withColumn("store_key", hash_key(*store_cols))
        .select(
            "store_key",
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
        )
        .dropDuplicates(["store_key"])
    )

    supplier_cols = [
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ]
    dim_supplier = (
        df.withColumn("supplier_key", hash_key(*supplier_cols))
        .select(
            "supplier_key",
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country",
        )
        .dropDuplicates(["supplier_key"])
    )

    dim_date = (
        df.select("sale_date")
        .where(F.col("sale_date").isNotNull())
        .dropDuplicates(["sale_date"])
        .withColumn("date_key", F.date_format("sale_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("day", F.dayofmonth("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .withColumn("quarter", F.quarter("sale_date"))
        .withColumn("year", F.year("sale_date"))
        .select("date_key", "sale_date", "day", "month", "quarter", "year")
    )

    keyed = (
        df.withColumn("customer_key", hash_key(*customer_cols))
        .withColumn("seller_key", hash_key(*seller_cols))
        .withColumn("product_key", hash_key(*product_cols))
        .withColumn("store_key", hash_key(*store_cols))
        .withColumn("supplier_key", hash_key(*supplier_cols))
        .withColumn("date_key", F.date_format("sale_date", "yyyyMMdd").cast(IntegerType()))
    )
    fact_sales = keyed.select(
        F.col("raw_id").alias("sale_key"),
        "date_key",
        "customer_key",
        "seller_key",
        "product_key",
        "store_key",
        "supplier_key",
        "sale_quantity",
        "sale_total_price",
    )

    return {
        "dim_customer": dim_customer,
        "dim_seller": dim_seller,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "dim_supplier": dim_supplier,
        "dim_date": dim_date,
        "fact_sales": fact_sales,
    }


def build_reports(dims):
    fact = dims["fact_sales"]
    product = dims["dim_product"]
    customer = dims["dim_customer"]
    store = dims["dim_store"]
    supplier = dims["dim_supplier"]
    date = dims["dim_date"]

    product_base = (
        fact.join(product, "product_key", "left")
        .groupBy("product_name", "product_category")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.count("*").alias("sales_count"),
            F.avg("product_rating").alias("avg_rating"),
            F.sum("product_reviews").alias("reviews_count"),
        )
    )
    product_report = (
        product_base.withColumn("report_product_key", hash_key("product_name", "product_category"))
        .withColumn("sales_rank", F.dense_rank().over(Window.orderBy(F.desc("total_quantity_sold"))))
        .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))))
        .withColumn("category_revenue", F.sum("total_revenue").over(Window.partitionBy("product_category")))
        .select(
            "report_product_key",
            "product_name",
            "product_category",
            "total_revenue",
            "total_quantity_sold",
            "sales_count",
            "avg_rating",
            "reviews_count",
            "sales_rank",
            "revenue_rank",
            "category_revenue",
        )
    )

    customer_report = (
        fact.join(customer, "customer_key", "left")
        .groupBy(
            "customer_key",
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_country",
        )
        .agg(
            F.sum("sale_total_price").alias("total_purchase_amount"),
            F.count("*").alias("orders_count"),
            F.sum("sale_quantity").alias("items_count"),
            F.avg("sale_total_price").alias("avg_check"),
        )
        .withColumn("purchase_rank", F.dense_rank().over(Window.orderBy(F.desc("total_purchase_amount"))))
        .withColumn("country_customer_count", F.count("*").over(Window.partitionBy("customer_country")))
    )

    monthly = (
        fact.join(date, "date_key", "left")
        .groupBy("year", "month")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("*").alias("orders_count"),
            F.sum("sale_quantity").alias("items_count"),
            F.avg("sale_total_price").alias("avg_order_amount"),
        )
        .withColumn("period_start", F.to_date(F.concat_ws("-", F.col("year"), F.col("month"), F.lit("01"))))
    )
    time_window = Window.orderBy("year", "month")
    time_report = (
        monthly.withColumn("year_revenue", F.sum("total_revenue").over(Window.partitionBy("year")))
        .withColumn("prev_month_revenue", F.lag("total_revenue").over(time_window))
        .withColumn("revenue_diff_to_prev_month", F.col("total_revenue") - F.col("prev_month_revenue"))
        .select(
            "period_start",
            "year",
            "month",
            "total_revenue",
            "year_revenue",
            "orders_count",
            "items_count",
            "avg_order_amount",
            "prev_month_revenue",
            "revenue_diff_to_prev_month",
        )
    )

    store_report = (
        fact.join(store, "store_key", "left")
        .groupBy("store_name")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("*").alias("orders_count"),
            F.sum("sale_quantity").alias("items_count"),
            F.avg("sale_total_price").alias("avg_check"),
            F.countDistinct("store_city").alias("cities_count"),
            F.countDistinct("store_country").alias("countries_count"),
        )
        .withColumn("report_store_key", hash_key("store_name"))
        .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))))
        .select(
            "report_store_key",
            "store_name",
            "total_revenue",
            "orders_count",
            "items_count",
            "avg_check",
            "cities_count",
            "countries_count",
            "revenue_rank",
        )
    )

    supplier_report = (
        fact.join(supplier, "supplier_key", "left")
        .join(product, "product_key", "left")
        .groupBy("supplier_name")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.sum("sale_quantity").alias("items_count"),
            F.avg("product_price").alias("avg_product_price"),
            F.countDistinct("product_key").alias("products_count"),
            F.countDistinct("supplier_city").alias("cities_count"),
            F.countDistinct("supplier_country").alias("countries_count"),
        )
        .withColumn("report_supplier_key", hash_key("supplier_name"))
        .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))))
        .select(
            "report_supplier_key",
            "supplier_name",
            "total_revenue",
            "items_count",
            "avg_product_price",
            "products_count",
            "cities_count",
            "countries_count",
            "revenue_rank",
        )
    )

    quality_base = (
        fact.join(product, "product_key", "left")
        .groupBy("product_name", "product_category")
        .agg(
            F.avg("product_rating").alias("avg_rating"),
            F.sum("product_reviews").alias("reviews_count"),
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.sum("sale_total_price").alias("total_revenue"),
        )
    )
    corr_row = quality_base.agg(F.corr("avg_rating", "total_quantity_sold").alias("rating_sales_correlation")).first()
    rating_sales_correlation = corr_row["rating_sales_correlation"] if corr_row else None
    quality_report = (
        quality_base.withColumn("report_product_key", hash_key("product_name", "product_category"))
        .withColumn("rating_rank_desc", F.dense_rank().over(Window.orderBy(F.desc("avg_rating"))))
        .withColumn("rating_rank_asc", F.dense_rank().over(Window.orderBy(F.asc("avg_rating"))))
        .withColumn("reviews_rank", F.dense_rank().over(Window.orderBy(F.desc("reviews_count"))))
        .withColumn("rating_sales_correlation", F.lit(rating_sales_correlation).cast(DoubleType()))
        .select(
            "report_product_key",
            "product_name",
            "product_category",
            "avg_rating",
            "reviews_count",
            "total_quantity_sold",
            "total_revenue",
            "rating_rank_desc",
            "rating_rank_asc",
            "reviews_rank",
            "rating_sales_correlation",
        )
    )

    return {
        "sales_by_products": product_report,
        "sales_by_customers": customer_report,
        "sales_by_time": time_report,
        "sales_by_stores": store_report,
        "sales_by_suppliers": supplier_report,
        "product_quality": quality_report,
    }


def main() -> None:
    spark = build_spark()
    try:
        raw = read_postgres_table(spark, "raw.mock_data")
        source = typed_raw(raw).cache()

        dims = build_dimensions(source)
        for name, frame in dims.items():
            write_postgres_table(frame, f"dwh.{name}")

        reports = build_reports(dims)
        for name, frame in reports.items():
            write_clickhouse_table(frame, f"bigdata.{name}")

        print("ETL finished")
        print(f"Raw rows: {source.count()}")
        for name, frame in dims.items():
            print(f"PostgreSQL dwh.{name}: {frame.count()} rows")
        for name, frame in reports.items():
            print(f"ClickHouse bigdata.{name}: {frame.count()} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
