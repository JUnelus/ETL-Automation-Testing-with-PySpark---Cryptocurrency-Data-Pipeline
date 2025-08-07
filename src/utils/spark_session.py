from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging
import os


def create_spark_session(app_name: str = "CryptoETL", local_mode: bool = True) -> SparkSession:
    """
    Create an optimized PySpark session for ETL processing
    Demonstrates production Spark configuration skills
    """

    # Spark configuration for optimal performance
    conf = SparkConf()
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    if local_mode:
        # Local development settings
        conf.set("spark.master", "local[*]")
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.driver.maxResultSize", "2g")
        conf.set("spark.sql.shuffle.partitions", "4")

    # Create a Spark session
    spark = SparkSession.builder \
        .appName(app_name) \
        .config(conf=conf) \
        .getOrCreate()

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    logging.info(f"Spark session created: {app_name}")
    logging.info(f"Spark version: {spark.version}")

    return spark
