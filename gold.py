from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import os
import subprocess

# ❌ HARDCODED CREDENTIALS (security issue)
DB_USER = "admin"
DB_PASSWORD = "SuperSecretPassword123"  # should be env var or secret manager

# ❌ HARDCODED PATHS (bad practice)
SILVER_PATH = "/mnt/silver/customers"
GOLD_PATH = "/mnt/gold/customers"

# ❌ MAGIC NUMBER
TOP_N_CUSTOMERS = 1000


def get_spark():
    # ❌ Hardcoded master & app name
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("GoldLayerJob")
        .getOrCreate()
    )


def unsafe_shell_call():
    # ❌ SECURITY ISSUE: shell=True
    subprocess.call("ls -ltr /mnt", shell=True)


def build_gold_layer(spark):
    df = spark.read.format("delta").load(SILVER_PATH)

    # ❌ PERFORMANCE ISSUE: count() used multiple times
    total_records = df.count()
    print(f"Total records: {total_records}")

    if df.count() > 0:
        print("Data exists")

    # ❌ PERFORMANCE ISSUE: collect() on full DataFrame
    rows = df.collect()
    for row in rows:
        print(row["id"])

    # ❌ NO NULL HANDLING / DATA VALIDATION
    gold_df = (
        df.groupBy("country")
        .agg(
            spark_sum(col("spend")).alias("total_spend")
        )
        .orderBy(col("total_spend").desc())
        .limit(TOP_N_CUSTOMERS)  # ❌ magic number usage
    )

    return gold_df


def write_gold_layer(df):
    # ❌ overwrite without checks / partitioning
    df.write.format("delta").mode("overwrite").save(GOLD_PATH)


def main():
    spark = get_spark()

    # ❌ Printing sensitive info
    print(f"Connecting as user {DB_USER} with password {DB_PASSWORD}")

    unsafe_shell_call()

    gold_df = build_gold_layer(spark)
    write_gold_layer(gold_df)

    spark.stop()


if __name__ == "__main__":
    main()
