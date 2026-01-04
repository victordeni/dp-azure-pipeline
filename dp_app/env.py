import os
import sys
from azure.storage.blob import BlobServiceClient
from .logging_utils import log


def load_env():
    """
    Read environment variables, log basic info
    and create the BlobServiceClient.
    """
    account_name = os.getenv("STORAGE_ACCOUNT")
    account_key = os.getenv("STORAGE_ACCOUNT_KEY")

    bronze_container = os.getenv("BRONZE_CONTAINER", "dp-bronze")
    silver_container = os.getenv("SILVER_CONTAINER", "dp-silver")
    gold_container = os.getenv("GOLD_CONTAINER", "dp-gold")

    log("🔁 IMAGE VERSION = V10-MODULAR")
    log("✅ DP pipeline job starting")
    log(f"📦 STORAGE_ACCOUNT = {account_name}")
    log(f"🥉 BRONZE_CONTAINER = {bronze_container}")
    log(f"🥈 SILVER_CONTAINER = {silver_container}")
    log(f"🥇 GOLD_CONTAINER   = {gold_container}")

    if not account_name or not account_key:
        log("❌ STORAGE_ACCOUNT or STORAGE_ACCOUNT_KEY not set")
        sys.exit(1)

    account_url = f"https://{account_name}.blob.core.windows.net"
    blob_service = BlobServiceClient(account_url=account_url, credential=account_key)

    return (
        account_name,
        account_key,
        bronze_container,
        silver_container,
        gold_container,
        account_url,
        blob_service,
    )
