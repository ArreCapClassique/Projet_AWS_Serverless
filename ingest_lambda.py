import json
import boto3
import urllib.request
import datetime

# =========================
# Config
# =========================
RAW_BUCKET = "weather-raw-nmk4945a"

API_URL = (
    "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/"
    "42-station-meteo-toulouse-parc-compans-cafarelli/records"
    "?limit=1&order_by=heure_utc%20desc"
)


def lambda_handler(event, context):
    """
    Fetch latest weather record from Toulouse Metropole API and store raw JSON in S3.
    Partitioning: YYYY/MM/DD/weather_HHMMSS.json (UTC)
    """
    s3 = boto3.client("s3")

    try:
        print(f"[INGEST] Fetching API: {API_URL}")

        with urllib.request.urlopen(API_URL) as response:
            status_code = response.getcode()

            if status_code != 200:
                error_msg = f"[INGEST] API request failed, status_code={status_code}"
                print(error_msg)
                return {"statusCode": status_code, "body": json.dumps(error_msg)}

            raw_json = response.read().decode("utf-8")
            payload = json.loads(raw_json)

        results = payload.get("results", [])
        if not results:
            print("[INGEST] Warning: API returned empty results")
            return {"statusCode": 204, "body": json.dumps("No content found in API")}

        # Keep only the latest record (dict), not the whole list
        current_weather = results[0]

        now = datetime.datetime.now(datetime.timezone.utc)
        partition_path = now.strftime("%Y/%m/%d")
        filename = f"weather_{now.strftime('%H%M%S')}.json"
        key = f"{partition_path}/{filename}"

        print(f"[INGEST] Uploading to s3://{RAW_BUCKET}/{key}")
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=key,
            Body=json.dumps(current_weather, ensure_ascii=False),
            ContentType="application/json",
        )

        print("[INGEST] Upload success")
        return {
            "statusCode": 200,
            "body": json.dumps(f"Successfully ingested data to {key}"),
        }

    except Exception as e:
        print(f"[INGEST] Fatal error: {str(e)}")
        raise
