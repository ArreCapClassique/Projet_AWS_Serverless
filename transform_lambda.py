import csv
import io
import json

import boto3
from urllib.parse import unquote_plus

DEST_BUCKET = "weather-clean-nmk4945a"


def _iter_s3_records(event: dict):
    """
    Normalize S3 trigger event shape:
    AWS S3 events usually look like: {"Records": [ ... ]}.
    """
    return (event or {}).get("Records", [])


def _read_s3_json(s3, bucket: str, key: str) -> dict:
    resp = s3.get_object(Bucket=bucket, Key=key)
    raw = resp["Body"].read().decode("utf-8")
    return json.loads(raw)


def _extract_weather_fields(data: dict, context) -> dict:
    # Flatten the already-extracted record from the ingest stage (results[0])
    return {
        "station_id": data.get("id"),
        "timestamp_utc": data.get("heure_utc"),
        "temperature_c": data.get("temperature_en_degre_c"),
        "humidity": data.get("humidite"),
        "pressure": data.get("pression"),
        "rain_intensity": data.get("pluie"),
        "wind_speed": data.get("force_moyenne_du_vecteur_vent"),
        "wind_direction": data.get("direction_du_vecteur_vent_moyen"),
        "ingestion_time": getattr(context, "aws_request_id", None),
    }


def _to_single_row_csv(row: dict) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(row.keys()))
    writer.writeheader()
    writer.writerow(row)
    return buf.getvalue()


def _dest_key_from_source(source_key: str) -> str:
    # Keep partition path, only swap suffix
    return (
        source_key[:-5] + ".csv"
        if source_key.lower().endswith(".json")
        else f"{source_key}.csv"
    )


def lambda_handler(event, context):
    """
    Triggered by S3 ObjectCreated on the raw bucket.
    Reads raw JSON record, flattens fields, writes 1-row CSV to the clean bucket.
    """
    s3 = boto3.client("s3")

    for rec in _iter_s3_records(event):
        try:
            src_bucket = rec["s3"]["bucket"]["name"]
            src_key = unquote_plus(rec["s3"]["object"]["key"])

            print(f"[TRANSFORM] Processing s3://{src_bucket}/{src_key}")

            data = _read_s3_json(s3, src_bucket, src_key)
            row = _extract_weather_fields(data, context)
            csv_body = _to_single_row_csv(row)

            dest_key = _dest_key_from_source(src_key)

            print(f"[TRANSFORM] Writing CSV to s3://{DEST_BUCKET}/{dest_key}")
            s3.put_object(
                Bucket=DEST_BUCKET,
                Key=dest_key,
                Body=csv_body,
                ContentType="text/csv",
            )

        except Exception as e:
            print(
                f"[TRANSFORM] Failed record (bucket/key): {rec.get('s3', {})} error={e}"
            )
            raise

    return {"statusCode": 200, "body": json.dumps({"message": "Processing Complete"})}
