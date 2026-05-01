"""
PermitGuard - Stage 1: BigQuery extraction (Section 4.1).

Executes permit_traces_query.sql against Google BigQuery and saves the
result as a local JSONL file under data/raw_traces/, ready for the
Stage 2 data processor (pipeline/data_processing/data_processor.py).

Prerequisites
-------------
1. A Google Cloud Platform project with the BigQuery API enabled.
2. Local authentication (pick ONE):
   (a) Easiest: install gcloud SDK and run
           gcloud auth application-default login
   (b) Or set GOOGLE_APPLICATION_CREDENTIALS to a service-account JSON.
3. Set GCP_PROJECT_ID in .env (used as the billing project).

Note: the public dataset bigquery-public-data.crypto_ethereum is free,
but BigQuery still bills the *scanned* bytes against your project (1 TB
free per month is included).

Usage
-----
    python pipeline/bigquery_extraction/run_extraction.py
"""

import os
import sys
import json
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")

PIPELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PIPELINE_DIR.parents[1]

SQL_FILE = PIPELINE_DIR / "permit_traces_query.sql"
OUTPUT_DIR = PROJECT_ROOT / "data" / "pipeline_output" / "raw_traces"
OUTPUT_FILE = OUTPUT_DIR / "permit_traces.jsonl"


def _serialize(value):
    """Convert non-JSON-serializable BigQuery values."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, list):
        return [_serialize(v) for v in value]
    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    return value


def main():
    if not PROJECT_ID:
        print("[Error] Please set GCP_PROJECT_ID in .env (your GCP billing project ID).")
        sys.exit(1)

    if not SQL_FILE.exists():
        print(f"[Error] SQL file not found: {SQL_FILE}")
        sys.exit(1)

    try:
        from google.cloud import bigquery
    except ImportError:
        print("[Error] google-cloud-bigquery is not installed.")
        print("        Run: pip install google-cloud-bigquery")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[BigQuery] Project : {PROJECT_ID}")
    print(f"[BigQuery] SQL     : {SQL_FILE.name}")
    print(f"[BigQuery] Output  : {OUTPUT_FILE}")

    client = bigquery.Client(project=PROJECT_ID)
    sql = SQL_FILE.read_text(encoding="utf-8")

    print("[BigQuery] Submitting query (this may take a while)...")
    job = client.query(sql)
    rows = job.result()

    bytes_processed = job.total_bytes_processed or 0
    print(f"[BigQuery] Bytes scanned: {bytes_processed / (1024 ** 3):.3f} GB")

    count = 0
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        for row in rows:
            record = {k: _serialize(v) for k, v in dict(row.items()).items()}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
            if count % 1000 == 0:
                sys.stdout.write(f"\r[Progress] {count} rows written...")
                sys.stdout.flush()

    print(f"\n[Done] {count} rows saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
