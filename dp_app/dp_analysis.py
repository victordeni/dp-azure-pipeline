import pandas as pd
import pipeline_dp
from .logging_utils import log


def compute_baseline(df_silver):
    log("📊 Computing non-DP baseline")

    pdf = df_silver.toPandas()

    baseline = {}

    baseline["sessions_by_continent"] = (
        pdf.groupby("continent")
        .size()
        .reset_index(name="raw_count")
    )

    baseline["avg_time_by_device"] = (
        pdf.groupby("device")["total_time_on_page"]
        .mean()
        .reset_index(name="raw_avg_time")
    )

    baseline["avg_pages_by_browser"] = (
        pdf.groupby("browser")["num_pageviews"]
        .mean()
        .reset_index(name="raw_avg_pages")
    )

    log("✅ Non-DP baseline computed")
    return baseline, pdf



def run_dp_sessions_by_continent(pdf):
    log("🔒 DP: sessions by continent")

    data = pdf[["continent"]].dropna()
    data["privacy_id"] = range(len(data))
    data["value"] = 1

    engine = pipeline_dp.DPEngine(
        budget_accountant=pipeline_dp.NaiveBudgetAccountant(5, 1e-6),
        backend=pipeline_dp.LocalBackend(),
    )

    params = pipeline_dp.AggregateParams(
        metrics=[pipeline_dp.Metrics.COUNT],
        max_partitions_contributed=1,
        max_contributions_per_partition=1,
    )

    dp_result = engine.aggregate(
        col=data.to_dict("records"),
        params=params,
        data_extractors=pipeline_dp.DataExtractors(
            privacy_id_extractor=lambda r: r["privacy_id"],
            partition_extractor=lambda r: r["continent"],
            value_extractor=lambda r: r["value"],
        ),
    )

    engine._budget_accountant.compute_budgets()
    return pd.DataFrame(
        [(k, v.count) for k, v in dp_result],
        columns=["continent", "dp_count"]
    )

def run_dp_avg_time_by_device(pdf):
    log("🔒 DP: avg time by device")

    data = pdf[["device", "total_time_on_page"]].dropna()
    data["privacy_id"] = range(len(data))

    engine = pipeline_dp.DPEngine(
        budget_accountant=pipeline_dp.NaiveBudgetAccountant(5, 1e-6),
        backend=pipeline_dp.LocalBackend(),
    )

    params = pipeline_dp.AggregateParams(
        metrics=[pipeline_dp.Metrics.MEAN],
        max_partitions_contributed=1,
        max_contributions_per_partition=1,
        min_value=0,
        max_value=600_000,
    )


    dp_result = engine.aggregate(
        col=data.to_dict("records"),
        params=params,
        data_extractors=pipeline_dp.DataExtractors(
            privacy_id_extractor=lambda r: r["privacy_id"],
            partition_extractor=lambda r: r["device"],
            value_extractor=lambda r: r["total_time_on_page"],
        ),
    )

    engine._budget_accountant.compute_budgets()
    return pd.DataFrame(
        [(k, v.mean) for k, v in dp_result],
        columns=["device", "dp_avg_time"]
    )



def merge_results(raw_df: pd.DataFrame, dp_df: pd.DataFrame, key: str) -> pd.DataFrame:
    log("📊 Merging raw and DP results")

    merged = raw_df.merge(dp_df, on=key, how="left")

    # raw column = second column
    raw_col = merged.columns[1]
    # dp column = third column
    dp_col = merged.columns[2]

    merged["diff_absolute"] = merged[dp_col] - merged[raw_col]
    merged["diff_percent"] = (
        merged["diff_absolute"] / merged[raw_col] * 100
    ).round(2)

    log("✅ Merge done")
    return merged
