import datetime
import json
import urllib.request

import boto3

RAW_BUCKET = "weather-raw-nmk4945a"
API_URL = (
    "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/"
    "42-station-meteo-toulouse-parc-compans-cafarelli/records"
    "?limit=1&order_by=heure_utc%20desc"
)


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _s3_key_for_weather(now: datetime.datetime) -> str:
    # Partitioning: YYYY/MM/DD/weather_HHMMSS.json (UTC)
    return f"{now:%Y/%m/%d}/weather_{now:%H%M%S}.json"


def _fetch_latest_weather_record(api_url: str) -> dict:
    print(f"[INGEST] Fetching API: {api_url}")

    with urllib.request.urlopen(api_url) as resp:
        status = resp.getcode()
        if status != 200:
            raise RuntimeError(f"API request failed, status_code={status}")

        payload = json.loads(resp.read().decode("utf-8"))

    results = payload.get("results") or []
    if not results:
        # Keep this as a “soft” error so Lambda can return 204 instead of crashing.
        return {}

    return results[0]  # latest record only


def lambda_handler(event, context):
    """
    Fetch latest weather record from Toulouse Metropole API and store raw JSON in S3.
    Partitioning: YYYY/MM/DD/weather_HHMMSS.json (UTC)
    """
    s3 = boto3.client("s3")

    try:
        record = _fetch_latest_weather_record(API_URL)
        if not record:
            msg = "No content found in API (empty results)"
            print(f"[INGEST] Warning: {msg}")
            return {"statusCode": 204, "body": json.dumps(msg)}

        now = _utc_now()
        key = _s3_key_for_weather(now)

        print(f"[INGEST] Uploading to s3://{RAW_BUCKET}/{key}")
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=key,
            Body=json.dumps(record, ensure_ascii=False),
            ContentType="application/json",
        )

        print("[INGEST] Upload success")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Ingested", "s3_key": key}),
        }

    except Exception as e:
        print(f"[INGEST] Fatal error: {e}")
        raise
