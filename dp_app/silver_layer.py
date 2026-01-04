import os
from datetime import datetime
from pyspark.sql.functions import col
from .logging_utils import log


def build_silver_df(spark, pdf_raw):
    """
    Build Silver layer from Bronze pandas DataFrame.
    Silver = clean, typed, analytics-ready session data.
    """
    df_raw = spark.createDataFrame(pdf_raw)
    log(f"✅ df_raw Spark rows = {df_raw.count()}")
    df_raw.printSchema()

    log("🧩 Building Silver (session-level data)")

    df_silver = (
        df_raw.select(
            col("date"),
            col("continent"),
            col("country"),
            col("device"),
            col("browser"),
            col("os"),
            col("visit_number"),
            col("num_pageviews"),
            col("num_events"),
            col("total_time_on_page"),
        )
        # Basic sanity filters
        .filter(col("date").isNotNull())
        .filter(col("continent").isNotNull())
    )

    silver_count = df_silver.count()
    log(f"✅ Silver ready: {silver_count} rows")

    return df_silver, silver_count


def write_silver_parquet_and_upload(df_silver, blob_service, silver_container_name: str):
    """
    Write Silver to local Parquet and upload to the Silver container.
    Returns the run_id used in paths.
    """
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    silver_relative_path = f"sessions/run_id={run_id}"
    local_silver_dir = f"/workspace/silver_run_{run_id}"

    log(f"💾 Writing Silver locally to: {local_silver_dir}")
    df_silver.write.mode("overwrite").parquet(local_silver_dir)
    log("✅ Silver written locally")

    log("☁️ Uploading Silver Parquet files to dp-silver")
    silver_container = blob_service.get_container_client(silver_container_name)

    for root, dirs, files in os.walk(local_silver_dir):
        for fname in files:
            full_path = os.path.join(root, fname)
            rel_path = os.path.relpath(full_path, local_silver_dir)
            blob_name = f"{silver_relative_path}/{rel_path}".replace("\\", "/")

            log(f"⬆️ Uploading Silver file {blob_name}")
            with open(full_path, "rb") as f:
                silver_container.upload_blob(
                    name=blob_name,
                    data=f,
                    overwrite=True
                )

    log("✅ Silver uploaded to dp-silver")
    return run_id
