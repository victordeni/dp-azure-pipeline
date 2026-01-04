from dp_app.env import load_env
from dp_app.bronze_reader import read_bronze_to_pandas
from dp_app.spark_utils import create_spark_session
from dp_app.silver_layer import build_silver_df, write_silver_parquet_and_upload
from dp_app.dp_analysis import compute_baseline, run_dp, merge_results
from dp_app.gold_writer import save_gold_results
from dp_app.logging_utils import log
import sys


def main():
    (
        account_name,
        account_key,
        bronze_container,
        silver_container,
        gold_container,
        account_url,
        blob_service,
    ) = load_env()

    pdf_raw = read_bronze_to_pandas(account_url, account_key, bronze_container)

    spark = create_spark_session()
    df_silver, silver_count = build_silver_df(spark, pdf_raw)

    if silver_count == 0:
        log("⚠️ No rows left after silver filtering. Exiting.")
        sys.exit(0)

    run_id = write_silver_parquet_and_upload(df_silver, blob_service, silver_container)

    baseline, pdf_silver = compute_baseline(df_silver)
    df_dp_counts = run_dp(pdf_silver)
    merged = merge_results(baseline, df_dp_counts)

    save_gold_results(
        merged=merged,
        blob_service=blob_service,
        gold_container_name=gold_container,
        account_name=account_name,
        run_id=run_id,
    )


if __name__ == "__main__":
    main()
