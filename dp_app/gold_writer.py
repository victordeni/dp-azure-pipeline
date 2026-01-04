import json
import matplotlib.pyplot as plt
from .logging_utils import log


def save_gold_results(results_dict, blob_service, gold_container_name, account_name, run_id):
    """
    Save multiple DP results to Gold layer.
    `results_dict` is a dict: {analysis_name: DataFrame}
    """
    gold_container = blob_service.get_container_client(gold_container_name)

    for name, df in results_dict.items():
        log(f"💾 Saving Gold results for: {name}")

        results_json = df.to_dict(orient="records")
        results_bytes = json.dumps(results_json, indent=2).encode("utf-8")

        json_blob_name = f"dp_results/run_id={run_id}/{name}.json"
        gold_container.upload_blob(
            name=json_blob_name,
            data=results_bytes,
            overwrite=True,
        )

        log(f"✅ Uploaded JSON: {account_name}/{gold_container_name}/{json_blob_name}")

    log("🏁 Gold layer successfully written")
