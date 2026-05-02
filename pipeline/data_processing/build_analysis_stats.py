"""
Build monthly analysis statistics for paper figures (§4).

Reads cleaned permit CSVs, joins with the address label library,
computes monthly phishing statistics (Total_Tx, Phishing_Tx_Count,
Phishing_Tx_Pct, label distributions), and saves the summary CSV
to data/analysis_stats/.

Usage:
    python pipeline/data_processing/build_analysis_stats.py
    python pipeline/data_processing/build_analysis_stats.py --input-dir data/pipeline_output/cleaned
"""

import os
import sys
import re
import glob
import argparse

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from src.utils.config import CONFIG, DATA_DIR


def _safe_to_int(x):
    try:
        return int(float(x))
    except (ValueError, TypeError):
        return 0


def _get_dist(series):
    vc = series.value_counts(normalize=True)
    return {"L0": vc.get(0, 0), "L1": vc.get(1, 0), "L2": vc.get(2, 0)}


def build(input_dir=None, labels_csv=None, output_path=None):
    if input_dir is None:
        input_dir = CONFIG["PATHS"]["PIPELINE_CLEANED_DIR"]
    if labels_csv is None:
        labels_csv = CONFIG["PATHS"]["ADDRESS_LABELS"]
    if output_path is None:
        output_path = os.path.join(DATA_DIR, "analysis_stats", "monthly_permit_deep_analysis.csv")

    print(f"[Build Analysis Stats] Input:  {input_dir}")
    print(f"[Build Analysis Stats] Labels: {labels_csv}")

    df_labels = pd.read_csv(labels_csv, usecols=["address", "label", "nametag"], encoding_errors="replace")
    df_labels["address"] = df_labels["address"].str.lower().str.strip()
    label_map = df_labels.set_index("address")["label"].to_dict()
    print(f"  Label library loaded: {len(label_map)} addresses")

    pattern = os.path.join(input_dir, "cleaned_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"[Error] No cleaned CSV files found in {input_dir}")
        sys.exit(1)

    monthly_stats = []

    for file_path in files:
        match = re.search(r"(202[3-5])_(\d+)", os.path.basename(file_path))
        if not match:
            continue
        period = f"{match.group(1)}-{match.group(2).zfill(2)}"
        print(f"\n{'='*60}")
        print(f"  Processing: {period} ({os.path.basename(file_path)})")

        df = pd.read_csv(file_path)
        cols_to_lower = ["original_submitter", "relayer", "token_address"]
        for col in cols_to_lower:
            if col in df.columns:
                df[col] = df[col].str.lower().str.strip()

        if "permit_deadline" in df.columns:
            df["permit_deadline"] = df["permit_deadline"].apply(_safe_to_int)

        total_tx = len(df)
        if total_tx == 0:
            continue

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).astype("int64") // 10**9

        df["final_transfer_to"] = df.get("transfer_to", pd.Series(["none"] * len(df)))
        if "final_transfer_to" not in df.columns:
            df["final_transfer_to"] = "none"

        df["label_relayer"] = df["relayer"].map(label_map) if "relayer" in df.columns else None
        df["label_submitter"] = df["original_submitter"].map(label_map) if "original_submitter" in df.columns else None
        df["label_spender"] = df["permit_spender"].map(label_map) if "permit_spender" in df.columns else None
        df["label_tf_to"] = df["final_transfer_to"].map(label_map)

        unlabeled_relayers = df["label_relayer"].isna().sum() if "label_relayer" in df.columns else 0
        unlabeled_spenders = df["label_spender"].isna().sum() if "label_spender" in df.columns else 0

        is_phishing_tx = pd.Series([False] * len(df), index=df.index)
        for col in ["label_submitter", "label_relayer", "label_spender", "label_tf_to"]:
            if col in df.columns:
                is_phishing_tx = is_phishing_tx | (df[col] == 1)

        phishing_count = int(is_phishing_tx.sum())
        phishing_pct = phishing_count / total_tx

        print(f"    Total: {total_tx}  |  Phishing: {phishing_count} ({phishing_pct:.2%})")

        dist_sub = _get_dist(df["label_submitter"]) if "label_submitter" in df.columns else {"L0": 0, "L1": 0, "L2": 0}
        dist_spender = _get_dist(df["label_spender"]) if "label_spender" in df.columns else {"L0": 0, "L1": 0, "L2": 0}

        stats = {
            "Period": period,
            "Total_Tx": total_tx,
            "Phishing_Tx_Count": phishing_count,
            "Phishing_Tx_Pct": phishing_pct,
            "Sub_L0": dist_sub["L0"],
            "Sub_L1": dist_sub["L1"],
            "Sub_L2": dist_sub["L2"],
            "Spender_L0": dist_spender["L0"],
            "Spender_L1": dist_spender["L1"],
            "Spender_L2": dist_spender["L2"],
            "Unlabeled_Relayer_Count": unlabeled_relayers,
        }
        monthly_stats.append(stats)

    df_report = pd.DataFrame(monthly_stats)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_report.to_csv(output_path, index=False)

    print(f"\n{'='*60}")
    print(f"[Done] Monthly analysis stats saved: {output_path}")
    print(f"  {len(df_report)} monthly entries")
    print(f"  Total transactions: {df_report['Total_Tx'].sum()}")
    print(f"  Total phishing: {df_report['Phishing_Tx_Count'].sum()}")
    print(df_report[["Period", "Total_Tx", "Phishing_Tx_Count", "Phishing_Tx_Pct"]].to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build monthly analysis statistics for paper figures")
    parser.add_argument("--input-dir", default=None, help="Directory with cleaned_*.csv files")
    parser.add_argument("--labels", default=None, help="Path to labeled address CSV")
    parser.add_argument("--output", default=None, help="Output CSV path")
    args = parser.parse_args()
    build(args.input_dir, args.labels, args.output)
