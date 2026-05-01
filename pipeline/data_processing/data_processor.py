"""
Stage 2: BigQuery JSON/JSONL data cleaning and parsing.
Reads raw BigQuery exports, deduplicates, parses permit/transferFrom inputs,
matches atomic transfers, detects anomalous patterns, and outputs cleaned CSVs.

Two entry points:
  1. clean_from_jsonl()  — reads data/raw_traces/permit_traces.jsonl (from Stage 1)
                           and outputs to data/raw_traces/cleaned/  (isolated)
  2. clean_and_inspect_json_data() — reads external monthly JSON files (legacy mode)
"""

import os
import re
import glob
import ast
import datetime
import warnings
from collections import defaultdict

import pandas as pd
import numpy as np

warnings.filterwarnings('ignore')

# -- Import shared utilities --
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.utils.hex_parser import HexParser
from src.utils.config import CONFIG

BASE_PATH = CONFIG["BASE_PATH"]
BQ_PATH = os.path.join(BASE_PATH, "BigQuery_since20251230/new_data_0117")


def get_file_list(data_dir, name="*.json"):
    files = glob.glob(os.path.join(data_dir, name))
    def extract_date(fname):
        match = re.search(r'(202[3-4])_(\d+)', str(fname))
        if match:
            return (int(match.group(1)), int(match.group(2)))
        return (0, 0)
    return sorted(files, key=extract_date)


def extract_sort_key(filename):
    match = re.search(r'(202[3-4])\D+(\d+)', str(filename))
    if match:
        return (int(match.group(1)), int(match.group(2)))
    return (0, 0)


class AuditAnalyzer:
    """Per-file analysis: basic stats, mismatch ratio, atomicity, trace depth."""

    def __init__(self, df, filename):
        self.df = df
        self.filename = filename
        self.stats = {}

    def preprocess(self):
        self.df['trace_path'] = self.df['permit_trace'].apply(HexParser.safe_parse_trace)
        self.df['trace_depth'] = self.df['trace_path'].apply(len)
        self.df['is_mismatch'] = self.df['original_submitter'].str.lower() != self.df['permit_owner'].str.lower()

    def run_analysis(self):
        total = len(self.df)
        if total == 0:
            return {}

        self.stats['total_tx'] = total
        self.stats['direct_permit'] = len(self.df[self.df['permit_trace'].isna()])
        self.stats['unique_submitter'] = self.df['original_submitter'].nunique()

        relayed_df = self.df[self.df['original_submitter'] != self.df['relayer']]
        self.stats['unique_relayer'] = relayed_df['relayer'].nunique()
        self.stats['unique_tokens'] = self.df['token_address'].nunique()
        self.stats['unique_owner'] = self.df['permit_owner'].nunique()
        self.stats['unique_spender'] = self.df['permit_spender'].nunique()
        self.stats['unique_tf_from'] = self.df['transfer_from'].nunique()
        self.stats['unique_tf_to'] = self.df['transfer_to'].nunique()

        own_txs = self.df[self.df['original_submitter'].str.lower() == self.df['permit_owner'].str.lower()]
        self.stats['own_permit_ratio'] = len(own_txs) / total

        self.original_submitter = set(self.df['original_submitter'])
        self.relayer = set(self.df['relayer'])
        self.spender = set(self.df['permit_spender'])
        self.tf_to = set(self.df['transfer_to'])
        self.need_label_address = self.original_submitter | self.relayer | self.spender | self.tf_to

        mismatch = self.df[self.df['is_mismatch']]
        self.stats['actor_mismatch_ratio'] = len(mismatch) / total

        atomic = self.df[self.df['is_atomic']]
        self.stats['atomic_execution_ratio'] = len(atomic) / total
        self.stats['delayed_execution_ratio'] = 1 - self.stats['atomic_execution_ratio']

        direct = self.df[self.df['trace_depth'] == 0]
        self.stats['direct_call_ratio'] = len(direct) / total

        deep_nested = self.df[self.df['trace_depth'] >= 2]
        self.stats['deep_nested_ratio'] = len(deep_nested) / total

        return (self.stats, self.original_submitter, self.relayer,
                self.spender, self.tf_to, self.need_label_address)


def clean_and_inspect_json_data():
    """Main cleaning pipeline: reads raw JSONL, deduplicates, parses, saves cleaned CSV."""
    file_path = BQ_PATH
    print(f"Reading JSON data from: {file_path}")

    files = get_file_list(file_path)
    all_anomalies = []

    print(f"Processing {len(files)} monthly files...")

    for fp in files:
        match = re.search(r'(202[3-4])_(\d+)', fp)
        if not match:
            continue
        period = f"{match.group(1)}_{match.group(2).zfill(2)}"
        print(f"\n{'=' * 60}\nProcessing: {period} ({os.path.basename(fp)})")

        try:
            df = pd.read_json(fp, lines=True)
        except ValueError:
            df = pd.read_json(fp, lines=False)

        raw_count = len(df)

        text_cols = ['tx_hash', 'permit_trace', 'token_address', 'permit_input', 'relayer', 'original_submitter']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str).str.lower().str.strip()

        if 'tx_hash' in df.columns:
            df['tx_hash'] = df['tx_hash'].apply(lambda x: '0x' + x if x and not x.startswith('0x') else x)

        subset_cols = ['tx_hash', 'permit_trace']
        if df['permit_trace'].eq('').any():
            subset_cols.append('permit_input')
        df_cleaned = df.drop_duplicates(subset=subset_cols, keep='first')
        print(f"  Deduplicated: {raw_count} -> {len(df_cleaned)}")

        pure_records = []
        stats = {'total_permits': 0, 'matched_atomic': 0, 'delayed_only': 0, 'parse_error': 0}

        for _, row in df_cleaned.iterrows():
            stats['total_permits'] += 1
            permit_info = HexParser.parse_permit(row.get('permit_input', ''))
            if not permit_info or not permit_info.get('p_owner'):
                stats['parse_error'] += 1
                continue

            record = {
                'tx_hash': row.get('tx_hash'),
                'block_number': row.get('block_number'),
                'timestamp': row.get('timestamp'),
                'original_submitter': row.get('original_submitter'),
                'relayer': row.get('relayer'),
                'token_address': row.get('token_address'),
                'permit_owner': permit_info['p_owner'],
                'permit_spender': permit_info['p_spender'],
                'permit_value': permit_info['p_value'],
                'permit_deadline': permit_info['p_deadline'],
                'permit_trace': row.get('permit_trace'),
                'is_atomic': False,
                'transfer_from': None, 'transfer_to': None,
                'transfer_amount_hex': None, 'transfer_trace': None,
            }

            transfer_list = row.get('transfer_list')
            matched = None
            if isinstance(transfer_list, list):
                for tf_item in transfer_list:
                    if not isinstance(tf_item, dict):
                        continue
                    tf_info = HexParser.parse_transfer(tf_item.get('tf_input'))
                    if tf_info and tf_info.get('tf_from') == permit_info['p_owner']:
                        matched = {
                            'from': tf_info['tf_from'], 'to': tf_info['tf_to'],
                            'amount': tf_info['tf_value'], 'trace': tf_item.get('tf_trace'),
                        }
                        break

            if matched:
                record.update({
                    'is_atomic': True, 'transfer_from': matched['from'],
                    'transfer_to': matched['to'], 'transfer_amount_hex': matched['amount'],
                    'transfer_trace': matched['trace'],
                })
                stats['matched_atomic'] += 1
            else:
                stats['delayed_only'] += 1

            pure_records.append(record)

        df_pure = pd.DataFrame(pure_records)

        print(f"  Atomic: {stats['matched_atomic']} | Delayed: {stats['delayed_only']} | Errors: {stats['parse_error']}")

        save_dir = os.path.join(BQ_PATH, "cleaned_data")
        os.makedirs(save_dir, exist_ok=True)
        output_path = os.path.join(save_dir, f"cleaned_{period}.csv")
        df_pure.to_csv(output_path, index=False)
        print(f"  Saved: {output_path}")


def run_analyse():
    """Run AuditAnalyzer on all cleaned monthly CSVs and produce a summary report."""
    FILE_PATH = os.path.join(BQ_PATH, "cleaned_data/")
    all_reports = []

    files = get_file_list(FILE_PATH, "cleaned_202*.csv")
    print(f"Analysing {len(files)} monthly files...")

    for fp in files:
        match = re.search(r'(202[3-4])_(\d+)', fp)
        if not match:
            continue
        period = f"{match.group(1)}_{match.group(2).zfill(2)}"

        try:
            df = pd.read_csv(fp)
            analyzer = AuditAnalyzer(df, fp)
            analyzer.preprocess()
            result, *_ = analyzer.run_analysis()
            result['file_name'] = os.path.basename(fp)
            all_reports.append(result)
            print(f"  {period}: {len(df)} txs")
        except Exception as e:
            print(f"  {period} failed: {e}")

    if all_reports:
        final_df = pd.DataFrame(all_reports)
        final_df['sort_key'] = final_df['file_name'].apply(extract_sort_key)
        final_df = final_df.sort_values(by='sort_key').drop(columns=['sort_key']).reset_index(drop=True)
        cols = ['file_name'] + [c for c in final_df.columns if c != 'file_name']
        final_df = final_df[cols]
        print(f"\nTotal permit transactions: {final_df['total_tx'].sum()}")
        print(final_df.to_string())


def _process_dataframe(df, output_dir, period_label="all"):
    """
    Core cleaning logic shared by both entry points.
    Takes a raw DataFrame (from BigQuery JSON/JSONL), deduplicates, parses
    permit/transferFrom inputs, matches atomic transfers, and saves cleaned CSV.
    Returns the cleaned DataFrame.
    """
    raw_count = len(df)

    text_cols = ['tx_hash', 'permit_trace', 'token_address', 'permit_input', 'relayer', 'original_submitter']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.lower().str.strip()

    if 'tx_hash' in df.columns:
        df['tx_hash'] = df['tx_hash'].apply(lambda x: '0x' + x if x and not x.startswith('0x') else x)

    subset_cols = ['tx_hash', 'permit_trace']
    if df['permit_trace'].eq('').any():
        subset_cols.append('permit_input')
    df_cleaned = df.drop_duplicates(subset=subset_cols, keep='first')
    print(f"  Deduplicated: {raw_count} -> {len(df_cleaned)}")

    pure_records = []
    stats = {'total_permits': 0, 'matched_atomic': 0, 'delayed_only': 0, 'parse_error': 0}

    for _, row in df_cleaned.iterrows():
        stats['total_permits'] += 1
        permit_info = HexParser.parse_permit(row.get('permit_input', ''))
        if not permit_info or not permit_info.get('p_owner'):
            stats['parse_error'] += 1
            continue

        record = {
            'tx_hash': row.get('tx_hash'),
            'block_number': row.get('block_number'),
            'timestamp': row.get('timestamp'),
            'original_submitter': row.get('original_submitter'),
            'relayer': row.get('relayer'),
            'token_address': row.get('token_address'),
            'permit_owner': permit_info['p_owner'],
            'permit_spender': permit_info['p_spender'],
            'permit_value': permit_info['p_value'],
            'permit_deadline': permit_info['p_deadline'],
            'permit_trace': row.get('permit_trace'),
            'is_atomic': False,
            'transfer_from': None, 'transfer_to': None,
            'transfer_amount_hex': None, 'transfer_trace': None,
        }

        transfer_list = row.get('transfer_list')
        matched = None
        if isinstance(transfer_list, list):
            for tf_item in transfer_list:
                if not isinstance(tf_item, dict):
                    continue
                tf_info = HexParser.parse_transfer(tf_item.get('tf_input'))
                if tf_info and tf_info.get('tf_from') == permit_info['p_owner']:
                    matched = {
                        'from': tf_info['tf_from'], 'to': tf_info['tf_to'],
                        'amount': tf_info['tf_value'], 'trace': tf_item.get('tf_trace'),
                    }
                    break

        if matched:
            record.update({
                'is_atomic': True, 'transfer_from': matched['from'],
                'transfer_to': matched['to'], 'transfer_amount_hex': matched['amount'],
                'transfer_trace': matched['trace'],
            })
            stats['matched_atomic'] += 1
        else:
            stats['delayed_only'] += 1

        pure_records.append(record)

    df_pure = pd.DataFrame(pure_records)
    print(f"  Atomic: {stats['matched_atomic']} | Delayed: {stats['delayed_only']} | Errors: {stats['parse_error']}")

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"cleaned_{period_label}.csv")
    df_pure.to_csv(output_path, index=False)
    print(f"  Saved: {output_path}")
    return df_pure


def clean_from_jsonl(input_path=None, output_dir=None):
    """
    Stage 2 entry point for the local pipeline.
    Reads a single JSONL file produced by Stage 1 (run_extraction.py),
    splits by month, cleans each chunk, and saves to data/raw_traces/cleaned/.
    Pre-built data under data/test_dataset/ etc. is never touched.
    """
    if input_path is None:
        input_path = CONFIG["PATHS"].get("RAW_TRACES_JSONL",
            os.path.join(CONFIG["BASE_PATH"], "raw_traces", "permit_traces.jsonl"))
    if output_dir is None:
        output_dir = CONFIG["PATHS"].get("PIPELINE_CLEANED_DIR",
            os.path.join(CONFIG["PATHS"]["PIPELINE_OUTPUT_DIR"], "cleaned"))

    print(f"[Stage 2] Reading JSONL: {input_path}")
    if not os.path.exists(input_path):
        print(f"[Error] File not found: {input_path}")
        return

    df = pd.read_json(input_path, lines=True)
    print(f"[Stage 2] Total rows loaded: {len(df)}")

    df['_ts'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    df['_month'] = df['_ts'].dt.strftime('%Y_%m').fillna('unknown')

    months = sorted(df['_month'].unique())
    print(f"[Stage 2] Found {len(months)} month(s): {', '.join(months)}")

    all_cleaned = []
    for month in months:
        chunk = df[df['_month'] == month].drop(columns=['_ts', '_month'])
        print(f"\n{'=' * 60}\nProcessing: {month} ({len(chunk)} rows)")
        cleaned = _process_dataframe(chunk, output_dir, period_label=month)
        all_cleaned.append(cleaned)

    if all_cleaned:
        merged = pd.concat(all_cleaned, ignore_index=True)
        merged_path = os.path.join(output_dir, "cleaned_all.csv")
        merged.to_csv(merged_path, index=False)
        print(f"\n[Stage 2] Merged dataset: {len(merged)} rows -> {merged_path}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Stage 2: Data cleaning & parsing")
    parser.add_argument("--mode", choices=["jsonl", "legacy"], default="jsonl",
                        help="jsonl: read data/raw_traces/permit_traces.jsonl (default); "
                             "legacy: read external monthly JSON files")
    parser.add_argument("--input", default=None, help="Override input file path")
    parser.add_argument("--output-dir", default=None, help="Override output directory")
    args = parser.parse_args()

    if args.mode == "jsonl":
        clean_from_jsonl(args.input, args.output_dir)
    else:
        clean_and_inspect_json_data()
