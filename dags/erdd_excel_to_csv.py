from __future__ import annotations

import io
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

BUCKET = "caenergy-fastrack"
INPUT_PREFIX = "ERDD/erdd-bq-ingestion/input_excel/"
OUTPUT_PREFIX = "ERDD/erdd-bq-ingestion/output_csv/"


def pick_latest_xlsx_key(s3: S3Hook) -> str:
    keys = s3.list_keys(bucket_name=BUCKET, prefix=INPUT_PREFIX) or []
    xlsx_keys = [k for k in keys if k.lower().endswith(".xlsx")]
    if not xlsx_keys:
        raise ValueError(f"No .xlsx files found under s3://{BUCKET}/{INPUT_PREFIX}")

    latest = max(xlsx_keys, key=lambda k: s3.get_key(k, BUCKET).last_modified)
    return latest


with DAG(
    dag_id="erdd_excel_to_csv",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # trigger manually first; we can add schedule later
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["erdd", "s3", "excel"],
) as dag:
    wait_for_excel = S3KeySensor(
        task_id="wait_for_excel",
        bucket_key=f"s3://{BUCKET}/{INPUT_PREFIX}*.xlsx",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    @task
    def extract_tabs_and_write_csv():
        s3 = S3Hook(aws_conn_id="aws_default")

        excel_key = pick_latest_xlsx_key(s3)
        excel_bytes = s3.get_key(excel_key, BUCKET).get()["Body"].read()

        xls = pd.ExcelFile(io.BytesIO(excel_bytes), engine="openpyxl")
        target_sheets = [name for name in xls.sheet_names if name.startswith("T_")]

        if not target_sheets:
            raise ValueError(f"No sheets starting with 'T_' found in {excel_key}")

        base_name = excel_key.split("/")[-1].rsplit(".", 1)[0]

        for sheet_name in target_sheets:
            df = pd.read_excel(xls, sheet_name=sheet_name)

            csv_buf = io.StringIO()
            df.to_csv(csv_buf, index=False)

            out_key = f"{OUTPUT_PREFIX}{base_name}/{sheet_name}.csv"
            s3.load_string(
                string_data=csv_buf.getvalue(),
                key=out_key,
                bucket_name=BUCKET,
                replace=True,
            )

        return {"excel_key": excel_key, "sheets_written": target_sheets}

    wait_for_excel >> extract_tabs_and_write_csv()