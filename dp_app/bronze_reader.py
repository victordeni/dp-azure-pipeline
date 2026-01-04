import json
import pandas as pd
from azure.storage.blob import ContainerClient
from .logging_utils import log


def read_bronze_to_pandas(account_url: str, account_key: str, bronze_container: str) -> pd.DataFrame:
    """
    Read all JSON events from Bronze and flatten them to a pandas DataFrame.
    (Same behavior as before, just with more columns)
    """
    log("📂 Listing bronze blobs under 'events/'")

    bronze_container_client = ContainerClient(
        account_url=account_url,
        container_name=bronze_container,
        credential=account_key,
    )

    rows = []

    for blob in bronze_container_client.list_blobs(name_starts_with="events/"):
        log(f"⬇️ Downloading {blob.name}")
        try:
            blob_data = bronze_container_client.download_blob(blob.name).readall()
            obj = json.loads(blob_data)
        except Exception as e:
            log(f"❌ Failed to load JSON from {blob.name}: {e}")
            continue

        raw_device = obj.get("device")
        geo = obj.get("geoNetwork", {})
        summary = obj.get("summary", {})

        if isinstance(raw_device, dict):
            browser = raw_device.get("browser")
            os_name = raw_device.get("operatingSystem")
            device_cat = raw_device.get("deviceCategory")
        else:
            browser = None
            os_name = None
            device_cat = raw_device

        row = {
            "visit_number": obj.get("visitNumber", 0),
            "date": obj.get("date", "unknown"),

            "continent": geo.get("continent", "unknown") if isinstance(geo, dict) else "unknown",
            "country": geo.get("country", "unknown") if isinstance(geo, dict) else "unknown",

            "browser": browser if browser is not None else "unknown",
            "os": os_name if os_name is not None else "unknown",
            "device": device_cat if device_cat is not None else "unknown",

            "num_pageviews": summary.get("num_pageviews", 0),
            "num_events": summary.get("num_events", 0),
            "total_time_on_page": summary.get("total_time_on_page", 0),
            }



        rows.append(row)

    log(f"✅ Loaded {len(rows)} rows from bronze")
    pdf_raw = pd.DataFrame(rows)
    log(f"✅ Raw pandas DataFrame shape: {pdf_raw.shape}")

    return pdf_raw
