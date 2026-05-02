"""
Build the evaluation dataset D_eval (§6.1).

Reads cleaned monthly permit CSVs, joins with the address label library to
derive ground-truth labels, then performs stratified sampling to produce a
balanced test set with a controllable malicious ratio.

Usage:
    python pipeline/data_processing/build_test_dataset.py
    python pipeline/data_processing/build_test_dataset.py --n 1000 --quarter 2024Q4 --ratio 0.15
    python pipeline/data_processing/build_test_dataset.py --input-dir data/pipeline_output/cleaned
"""

import os
import sys
import glob
import re
import random
import argparse

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from src.utils.config import CONFIG, DATA_DIR


def _get_file_list(data_dir, pattern="cleaned_202*.csv"):
    files = glob.glob(os.path.join(data_dir, pattern))
    def extract_date(fname):
        match = re.search(r"(202[3-5])_(\d+)", str(fname))
        if match:
            return (int(match.group(1)), int(match.group(2)))
        return (0, 0)
    return sorted(files, key=extract_date)


def _quarter_month_regex(quarter):
    """Convert quarter string like '2024Q4' to a regex matching year_month."""
    match = re.match(r"(\d{4})Q(\d)", quarter)
    if not match:
        print(f"[Error] Invalid quarter format: {quarter}, expected e.g. 2024Q4")
        sys.exit(1)
    year, q = match.group(1), int(match.group(2))
    start_month = (q - 1) * 3 + 1
    months = [str(start_month + i) for i in range(3)]
    month_pattern = "|".join(months)
    return rf"({year})_({month_pattern})\b"


def _check_malicious(row, malicious_addresses):
    addr_cols = ["original_submitter", "relayer", "permit_spender", "transfer_to"]
    for col in addr_cols:
        val = row.get(col)
        if val and val in malicious_addresses:
            return 1
    return 0


def build(input_dir=None, labels_csv=None, output_path=None,
          n=1000, quarter=None, malicious_ratio=0.15, seed=42,
          pool_per_month=10000):
    if input_dir is None:
        input_dir = CONFIG["PATHS"]["PIPELINE_CLEANED_DIR"]
    if labels_csv is None:
        labels_csv = CONFIG["PATHS"]["ADDRESS_LABELS"]
    if output_path is None:
        output_path = os.path.join(DATA_DIR, "test_dataset", "test_dataset.csv")

    print(f"[Build D_eval] Input: {input_dir}")
    print(f"[Build D_eval] Labels: {labels_csv}")
    print(f"[Build D_eval] Target: {n} txs, ratio={malicious_ratio}, seed={seed}")

    random.seed(seed)

    # Load malicious address set
    df_label = pd.read_csv(labels_csv, encoding_errors="replace")
    df_label["address"] = df_label["address"].str.lower().str.strip()
    malicious_addresses = set(df_label[df_label["label"] == 1]["address"].unique())
    print(f"  Malicious addresses: {len(malicious_addresses)}")

    # Scan monthly files, build candidate pool
    files = _get_file_list(input_dir)
    if not files:
        print(f"[Error] No cleaned CSV files found in {input_dir}")
        sys.exit(1)

    if quarter:
        qr = _quarter_month_regex(quarter)
        files = [f for f in files if re.search(qr, os.path.basename(f))]
        if not files:
            print(f"[Error] No files match quarter {quarter}")
            sys.exit(1)
        print(f"  Filtered to quarter {quarter}: {len(files)} files")

    candidate_rows = []
    for file_path in files:
        df_chunk = pd.read_csv(file_path, low_memory=False)
        cols_to_lower = ["original_submitter", "relayer", "permit_spender",
                         "token_address", "transfer_to"]
        for col in cols_to_lower:
            if col in df_chunk.columns:
                df_chunk[col] = df_chunk[col].astype(str).str.lower().str.strip()

        if len(df_chunk) > pool_per_month:
            sample = df_chunk.sample(pool_per_month, random_state=seed)
        else:
            sample = df_chunk
        candidate_rows.append(sample)
        print(f"  {os.path.basename(file_path)}: {len(df_chunk)} rows -> pool {len(sample)}")

    df_candidates = pd.concat(candidate_rows, ignore_index=True)
    print(f"  Candidate pool: {len(df_candidates)} rows")

    # Assign ground truth labels
    df_candidates["ground_truth_label"] = df_candidates.apply(
        lambda row: _check_malicious(row, malicious_addresses), axis=1
    )

    # Stratified sampling
    if malicious_ratio is not None and 0 < malicious_ratio < 1:
        target_malicious = int(n * malicious_ratio)
        target_benign = n - target_malicious

        df_mal = df_candidates[df_candidates["ground_truth_label"] == 1]
        df_ben = df_candidates[df_candidates["ground_truth_label"] == 0]

        print(f"  Pool malicious: {len(df_mal)}, benign: {len(df_ben)}")

        final_mal = df_mal.sample(n=min(len(df_mal), target_malicious), random_state=seed)
        final_ben = df_ben.sample(n=min(len(df_ben), target_benign), random_state=seed)

        df_final = pd.concat([final_mal, final_ben])
        df_final = df_final.sample(frac=1, random_state=seed).reset_index(drop=True)
    else:
        df_final = df_candidates.sample(
            n=min(len(df_candidates), n), random_state=seed
        ).reset_index(drop=True)

    # Save
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.to_csv(output_path, index=False)

    phish = (df_final["ground_truth_label"] == 1).sum()
    print(f"\n[Done] D_eval saved: {output_path} ({len(df_final)} rows)")
    print(f"  Phishing: {phish} ({phish/len(df_final)*100:.1f}%)")
    print(f"  Benign:   {len(df_final) - phish} ({(len(df_final)-phish)/len(df_final)*100:.1f}%)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build evaluation test dataset (D_eval)")
    parser.add_argument("--input-dir", default=None, help="Directory with cleaned_*.csv files")
    parser.add_argument("--labels", default=None, help="Path to labeled address CSV")
    parser.add_argument("--output", default=None, help="Output CSV path")
    parser.add_argument("--n", type=int, default=1000, help="Number of transactions to sample")
    parser.add_argument("--quarter", default=None, help="Filter by quarter, e.g. 2024Q4")
    parser.add_argument("--ratio", type=float, default=0.15, help="Malicious sample ratio (0-1)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--pool-per-month", type=int, default=10000,
                        help="Max rows to sample from each monthly file into candidate pool")
    args = parser.parse_args()
    build(args.input_dir, args.labels, args.output,
          args.n, args.quarter, args.ratio, args.seed, args.pool_per_month)
