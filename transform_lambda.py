import json
import boto3
import io

# Parquet writer deps (must be packaged with Lambda or provided via a Layer)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# =========================
# Config
# =========================
CLEAN_BUCKET = "weather-clean-nmk4945a"


def lambda_handler(event, context):
    """
    Triggered by S3 ObjectCreated events on RAW bucket.
    Reads raw JSON record, flattens fields, writes 1-row Parquet (Snappy) to CLEAN bucket.
    Keeps same partition path, changes suffix: .json -> .parquet
    """
    s3 = boto3.client("s3")

    try:
        records = event.get("Records", [])
        if not records:
            print("[CLEAN] Warning: No Records found in event")
            return {"statusCode": 400, "body": json.dumps("No Records in event")}

        for record in records:
            # 1) Parse S3 event metadata
            source_bucket = record["s3"]["bucket"]["name"]
            source_key = record["s3"]["object"]["key"]
            source_key = boto3.compat.urllib.parse.unquote_plus(source_key)

            print(f"[CLEAN] Processing s3://{source_bucket}/{source_key}")

            # 2) Read RAW JSON object
            obj = s3.get_object(Bucket=source_bucket, Key=source_key)
            json_content = obj["Body"].read().decode("utf-8")
            data = json.loads(json_content)

            # 3) Transform (flatten)
            extracted = {
                "station_id": data.get("id"),
                "timestamp_utc": data.get("heure_utc"),
                "temperature_c": data.get("temperature_en_degre_c"),
                "humidity": data.get("humidite"),
                "pressure": data.get("pression"),
                "rain_intensity": data.get("pluie"),
                "wind_speed": data.get("force_moyenne_du_vecteur_vent"),
                "wind_direction": data.get("direction_du_vecteur_vent_moyen"),
                "ingestion_request_id": context.aws_request_id,
            }

            # 4) Write Parquet (Snappy) to memory
            df = pd.DataFrame([extracted])
            table = pa.Table.from_pandas(df, preserve_index=False)

            out_buffer = io.BytesIO()
            pq.write_table(table, out_buffer, compression="snappy")
            out_buffer.seek(0)

            # 5) Destination key (keep same partition folders)
            dest_key = source_key.rsplit(".", 1)[0] + ".parquet"

            # 6) Upload to CLEAN bucket
            print(f"[CLEAN] Writing Parquet to s3://{CLEAN_BUCKET}/{dest_key}")
            s3.put_object(
                Bucket=CLEAN_BUCKET,
                Key=dest_key,
                Body=out_buffer.getvalue(),
                ContentType="application/octet-stream",
            )

        print("[CLEAN] Processing complete")
        return {"statusCode": 200, "body": json.dumps("Processing Complete")}

    except Exception as e:
        print(f"[CLEAN] Fatal error: {str(e)}")
        raise
