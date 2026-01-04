import logging
import azure.functions as func
import os
import json
from datetime import datetime, timezone

from azure.storage.blob import ContainerClient
from azure.identity import DefaultAzureCredential
import requests

app = func.FunctionApp()

# -----------------------------
# ENVIRONMENT
# -----------------------------
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
STORAGE_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
BRONZE_CONTAINER = os.getenv("BRONZE_CONTAINER", "dp-bronze")

# Pour compatibilité locale (HTTP direct vers Container App)
CONTAINERAPP_TRIGGER_URL = os.getenv("CONTAINERAPP_TRIGGER_URL")

# Pour appel ARM du Container Apps Job en prod
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")
RESOURCE_GROUP = os.getenv("RESOURCE_GROUP")
CONTAINERAPPS_JOB_NAME = os.getenv("CONTAINERAPPS_JOB_NAME", "dp-pipeline-job")

CHECKPOINT_BLOB = "trigger/checkpoint.json"

# Credential managé (en local il ignorera juste la MSI)
credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)


# -----------------------------
# Helper: read checkpoint
# -----------------------------
def load_checkpoint(container: ContainerClient) -> int:
    try:
        blob = container.download_blob(CHECKPOINT_BLOB).readall()
        data = json.loads(blob)
        return data.get("last_run_ts", 0)
    except Exception:
        # Pas encore de checkpoint
        return 0


# -----------------------------
# Helper: write checkpoint
# -----------------------------
def save_checkpoint(container: ContainerClient, ts: int) -> None:
    payload = json.dumps({"last_run_ts": ts}).encode("utf-8")
    container.upload_blob(CHECKPOINT_BLOB, payload, overwrite=True)


# -----------------------------
# Helper: trigger Spark job
# -----------------------------
def trigger_spark_job(logger: logging.Logger) -> bool:
    """
    Deux modes :
    - LOCAL : si CONTAINERAPP_TRIGGER_URL est défini -> POST HTTP direct (ton ancien comportement)
    - AZURE PROD : sinon, on appelle l'API ARM /jobs/<name>/start avec Managed Identity
    """

    # -------- Mode local / debug : URL directe --------
    if CONTAINERAPP_TRIGGER_URL:
        try:
            logger.info(f"🚀 Triggering Spark job via HTTP URL: {CONTAINERAPP_TRIGGER_URL}")
            resp = requests.post(CONTAINERAPP_TRIGGER_URL, timeout=10)
            logger.info(f"Spark HTTP response: {resp.status_code} {resp.text}")
            return resp.status_code in (200, 202)
        except Exception as e:
            logger.error(f"❌ Failed calling Spark job via HTTP: {e}", exc_info=True)
            return False

    # -------- Mode ARM / Managed Identity --------
    if not (SUBSCRIPTION_ID and RESOURCE_GROUP and CONTAINERAPPS_JOB_NAME):
        logger.error("❌ Missing SUBSCRIPTION_ID / RESOURCE_GROUP / CONTAINERAPPS_JOB_NAME")
        return False

    api_version = "2024-02-02-preview"
    url = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}"
        f"/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.App/jobs/{CONTAINERAPPS_JOB_NAME}/start"
        f"?api-version={api_version}"
    )

    try:
        # Token via Managed Identity (sur Azure) ou credentials dev (en local)
        token = credential.get_token("https://management.azure.com/.default").token
        headers = {
            "Authorization": f"Bearer {token}"
        }

        logger.info(f"🚀 Triggering Container Apps Job via ARM: {url}")
        resp = requests.post(url, headers=headers, timeout=30)
        logger.info(f"ARM job start response: {resp.status_code} {resp.text}")

        return resp.status_code in (200, 202)

    except Exception as e:
        logger.error(f"❌ Failed calling Container Apps Job via ARM: {e}", exc_info=True)
        return False


# -----------------------------
# TIMER TRIGGER FUNCTION
# -----------------------------
@app.function_name(name="check_bronze_trigger")
@app.schedule(schedule="*/30 * * * * *", arg_name="mytimer", run_on_startup=False)
def check_bronze_trigger(mytimer: func.TimerRequest) -> None:
    logging.info("⏳ Function B triggered — checking Bronze container")

    if not STORAGE_ACCOUNT or not STORAGE_KEY:
        logging.error("❌ Missing STORAGE_ACCOUNT or STORAGE_ACCOUNT_KEY env vars")
        return

    account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
    bronze = ContainerClient(
        account_url=account_url,
        container_name=BRONZE_CONTAINER,
        credential=STORAGE_KEY,
    )

    # ---- 1. Load checkpoint
    last_run_ts = load_checkpoint(bronze)
    logging.info(f"📌 Last checkpoint = {last_run_ts}")

    # ---- 2. Find new blobs
    new_blobs = []
    for blob in bronze.list_blobs(name_starts_with="events/"):
        if blob.last_modified.timestamp() > last_run_ts:
            new_blobs.append(blob.name)

    logging.info(f"🆕 Found {len(new_blobs)} new event files")

    if len(new_blobs) == 0:
        logging.info("➡️ No new data — Spark job NOT triggered")
        return

    # ---- 3. Trigger Spark container job
    job_ok = trigger_spark_job(logging)

    if not job_ok:
        logging.error("❌ Spark job trigger failed — checkpoint NOT updated")
        return

    # ---- 4. Save new checkpoint timestamp
    now_ts = int(datetime.now(timezone.utc).timestamp())
    save_checkpoint(bronze, now_ts)
    logging.info(f"📌 Updated checkpoint = {now_ts}")
