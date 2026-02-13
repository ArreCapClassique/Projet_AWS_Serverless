import datetime
import json
import urllib.request
import boto3
import os

RAW_BUCKET = os.environ.get("BUCKET_NAME")
API_URL = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/42-station-meteo-toulouse-parc-compans-cafarelli/records?limit=1&order_by=heure_utc%20desc"


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    try:
        with urllib.request.urlopen(API_URL) as resp:
            if resp.getcode() != 200:
                return {"statusCode": 500}
            data = json.loads(resp.read().decode("utf-8"))

        results = data.get("results") or []
        if not results:
            return {"statusCode": 204}

        record = results[0]
        now = datetime.datetime.now(datetime.timezone.utc)
        key = f"raw-data/{now:%Y/%m/%d}/weather_{now:%H%M%S}.json"

        s3.put_object(
            Bucket=RAW_BUCKET, Key=key, Body=json.dumps(record, ensure_ascii=False)
        )
        return {"statusCode": 200}
    except Exception as e:
        print(e)
        raise
