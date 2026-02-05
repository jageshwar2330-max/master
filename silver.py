from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    to_timestamp,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def read_bronze_table(spark: SparkSession, path: str):
    return spark.read.format("delta").load(path)


def transform_to_silver(df):
    """
    Apply silver-layer transformations:
    - Select required columns
    - Cast data types
    - Clean strings
    - Remove duplicates
    - Add audit columns
    """

    transformed_df = (
        df
        .select(
            col("id").cast(IntegerType()).alias("id"),
            trim(lower(col("name"))).alias("name"),
            trim(lower(col("email"))).alias("email"),
            to_timestamp(col("created_at")).alias("created_at")
        )
        .dropDuplicates(["id"])
        .withColumn("silver_processed_at", current_timestamp())
    )

    return transformed_df


def write_silver_table(df, path: str):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def main():
    BRONZE_PATH = "/mnt/bronze/customers"
    SILVER_PATH = "/mnt/silver/customers"

    spark = get_spark_session("Silver Layer Transformation")

    bronze_df = read_bronze_table(spark, BRONZE_PATH)

    silver_df = transform_to_silver(bronze_df)

    write_silver_table(silver_df, SILVER_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
