from pyspark.sql import SparkSession
from .logging_utils import log


def create_spark_session() -> SparkSession:
    log("🚀 Starting Spark session")
    spark = SparkSession.builder.appName("dp-pipeline").getOrCreate()
    log("✅ Spark session started")
    return spark
