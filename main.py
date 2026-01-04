from dp_app.env import load_env
from dp_app.bronze_reader import read_bronze_to_pandas
from dp_app.spark_utils import create_spark_session
from dp_app.silver_layer import build_silver_df, write_silver_parquet_and_upload
from dp_app.dp_analysis import compute_baseline, run_dp, merge_results
from dp_app.gold_writer import save_gold_results
from dp_app.logging_utils import log

def main():
    # 1. Load env
    (
        account_name,
        account_key,
        bronze_container,
        silver_container,
        gold_container,
        account_url,
        blob_service,
    ) = load_env()

    # 2. Read Bronze
    pdf_raw = read_bronze_to_pandas(account_url, account_key, bronze_container)

    # 3. Start Spark
    spark = create_spark_session()

    # 4. Build Silver
    df_silver, silver_count = build_silver_df(spark, pdf_raw)
    if silver_count == 0:
        log("⚠️ No data after silver transform. Exiting.")
        return

    # 5. Write silver parquet + upload
    run_id = write_silver_parquet_and_upload(df_silver, blob_service, silver_container)

    # 6. Compute baseline + DP
    baseline, pdf_silver = compute_baseline(df_silver)
    df_dp = run_dp(pdf_silver)

    # 7. Merge raw and DP
    merged = merge_results(baseline, df_dp)

    # 8. Save gold results
    save_gold_results(
        merged,
        blob_service,
        gold_container,
        account_name,
        run_id,
    )

    log("🏁 Pipeline completed successfully!")

if __name__ == "__main__":
    main()
