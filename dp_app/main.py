from dp_app.env import load_env
from dp_app.bronze_reader import read_bronze_to_pandas
from dp_app.spark_utils import create_spark_session
from dp_app.silver_layer import build_silver_df, write_silver_parquet_and_upload
from dp_app.dp_analysis import (
    compute_baseline,
    run_dp_sessions_by_continent,
    run_dp_avg_time_by_device,
    merge_results,
)
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
        
    if pdf_raw.empty:
        log("⚠️ No valid data found in Bronze. Exiting job.")
        return

    # 3. Start Spark
    spark = create_spark_session()

    # 4. Build Silver
    df_silver, silver_count = build_silver_df(spark, pdf_raw)
    if silver_count == 0:
        log("⚠️ No data after silver transform. Exiting.")
        return

    # 5. Write Silver
    run_id = write_silver_parquet_and_upload(
        df_silver, blob_service, silver_container
    )

    # 6. Compute baseline
    baseline, pdf = compute_baseline(df_silver)

    # 7. DP analyses
    dp_sessions = run_dp_sessions_by_continent(pdf)
    dp_time = run_dp_avg_time_by_device(pdf)

    # 8. Merge raw + DP
    merged_sessions = merge_results(
        baseline["sessions_by_continent"],
        dp_sessions,
        key="continent",
    )

    merged_time = merge_results(
        baseline["avg_time_by_device"],
        dp_time,
        key="device",
    )

    # 9. Save Gold results
    save_gold_results(
        {
            "sessions_by_continent": merged_sessions,
            "avg_time_by_device": merged_time,
        },
        blob_service,
        gold_container,
        account_name,
        run_id,
    )

    log("🏁 Pipeline completed successfully!")


if __name__ == "__main__":
    main()
