"""
Transfer feature extraction: compute topology matching and fund utilization
features for each permit transaction based on where funds actually flowed.
"""

import os
import re
import sys

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.utils.config import CONFIG
from pipeline.data_processing.data_processor import get_file_list

BASE_PATH = CONFIG["BASE_PATH"]
PATHS = CONFIG["PATHS"]


def run_transfer_analysis():
    print("Starting Transfer feature extraction...")

    # --- 1. Load knowledge bases ---
    label_path = PATHS["ADDRESS_LABELS"]
    df_labels = pd.read_csv(label_path, usecols=['address', 'label', 'nametag'], encoding_errors='replace')
    df_labels['address'] = df_labels['address'].str.lower().str.strip()
    label_map = df_labels.set_index('address')['label'].to_dict()

    token_path = PATHS["TOKEN_KB"]
    df_token_1000 = pd.read_csv(token_path, encoding_errors='replace')
    df_token_1000['contract_address'] = df_token_1000['contract_address'].str.lower().str.strip()
    decimal_map = df_token_1000.set_index('contract_address')['decimals'].to_dict()

    # --- 2. Load permit data ---
    FILE_PATH = PATHS["PIPELINE_CLEANED_DIR"]
    files = get_file_list(FILE_PATH, "cleaned_202*.csv")
    all_permit = []

    for fp in files:
        match = re.search(r'(202[3-4])_(\d+)', fp)
        if not match:
            continue
        try:
            df = pd.read_csv(fp)
            all_permit.append(df)
            print(f"  Loaded {match.group(1)}-{match.group(2).zfill(2)}: {len(df)} rows")
        except Exception as e:
            print(f"  Error: {e}")

    df_all = pd.concat(all_permit, ignore_index=True)

    # --- 3. Preprocessing ---
    cols_to_clean = ['permit_spender', 'permit_owner', 'original_submitter', 'relayer', 'transfer_to', 'token_address']
    for col in cols_to_clean:
        if col in df_all.columns:
            df_all[col] = df_all[col].astype(str).str.lower().str.strip()

    df_all['timestamp'] = pd.to_datetime(df_all['timestamp']).view('int64') // 10 ** 9
    df_all['final_transfer_to'] = 'none'
    df_all['final_transfer_amount'] = 0.0
    df_all['execution_status'] = 'Unused_Dormant'

    SPENDER_HISTORY_PATH = os.path.join(BASE_PATH, "verified_permit/labeled_address/1_order_0609/")
    TRANSFER_FROM_METHOD = "0x23b872dd"

    # --- 4. Resolve atomic & delayed ---
    mask_atomic = df_all['is_atomic'] == True
    df_all.loc[mask_atomic, 'final_transfer_to'] = df_all.loc[mask_atomic, 'transfer_to']
    df_all.loc[mask_atomic, 'execution_status'] = 'Atomic'

    def safe_hex_to_float(x):
        try:
            return float(x)
        except Exception:
            return 0.0

    if 'transfer_amount_hex' in df_all.columns:
        df_all.loc[mask_atomic, 'final_transfer_amount'] = \
            df_all.loc[mask_atomic, 'transfer_amount_hex'].apply(safe_hex_to_float)

    df_non_atomic = df_all[~mask_atomic].copy()
    unique_spenders = df_non_atomic['permit_spender'].unique()
    updates = []

    for i, spender in enumerate(unique_spenders):
        if i % 100 == 0:
            print(f"  Delayed backtrack: {i}/{len(unique_spenders)} spenders...")

        hist_file = os.path.join(SPENDER_HISTORY_PATH, f"{spender}/{spender}_external.csv")
        if not os.path.exists(hist_file):
            continue
        try:
            df_hist = pd.read_csv(hist_file, usecols=['txHash', 'timeStamp', 'from', 'input'])
        except Exception:
            continue
        if df_hist.empty:
            continue

        df_hist = df_hist[df_hist['input'].str.startswith(TRANSFER_FROM_METHOD, na=False)]
        current_permits = df_non_atomic[df_non_atomic['permit_spender'] == spender]

        for idx, row in current_permits.iterrows():
            victim = row['permit_owner'].replace("0x", "")
            for _, tx in df_hist[df_hist['timeStamp'] > row['timestamp']].iterrows():
                input_data = tx['input']
                param_from = input_data[10:74]
                if victim in param_from and param_from[24:].lower() == victim:
                    real_to = "0x" + input_data[74:138][24:].lower()
                    real_amt = float(int(input_data[138:202], 16))
                    updates.append((idx, real_to, real_amt))
                    break

    if updates:
        idx_list, to_list, amt_list = zip(*updates)
        df_all.loc[list(idx_list), 'final_transfer_to'] = list(to_list)
        df_all.loc[list(idx_list), 'final_transfer_amount'] = list(amt_list)
        df_all.loc[list(idx_list), 'execution_status'] = 'Used_Delayed'
        print(f"  Resolved {len(updates)} delayed drains")

    # --- 5. Topology classification ---
    conditions = [
        (df_all['final_transfer_to'] == 'none'),
        (df_all['final_transfer_to'] == df_all['permit_spender']),
        (df_all['final_transfer_to'] == df_all['original_submitter']),
        (df_all['final_transfer_to'] == df_all['relayer']),
        (df_all['final_transfer_to'] == df_all['permit_owner']),
        (df_all['final_transfer_to'] == df_all['token_address']),
    ]
    choices = ['No_Transfer', 'Self-Loop', 'Kickback/Sweep', 'Solver-Settlement', 'Self-Rescue', 'Reflection']
    df_all['feat_topology'] = np.select(conditions, choices, default='Third-Party Leakage')

    # --- 6. Utilization rate ---
    df_all['decimals'] = df_all['token_address'].map(decimal_map).fillna(18)
    permit_val = pd.to_numeric(df_all['permit_value'], errors='coerce').fillna(0.0)
    has_transfer = df_all['final_transfer_amount'] > 0
    is_infinite = permit_val > 1e50

    df_all['feat_utilization'] = 0.0
    normal_mask = has_transfer & (~is_infinite) & (permit_val > 0)
    df_all.loc[normal_mask, 'feat_utilization'] = (
        df_all.loc[normal_mask, 'final_transfer_amount'] / permit_val.loc[normal_mask]
    ).clip(upper=1.0)
    inf_used_mask = has_transfer & is_infinite
    df_all.loc[inf_used_mask, 'feat_utilization'] = 0.0001

    # --- 7. Label mapping & ground truth ---
    df_all['label_submitter'] = df_all['original_submitter'].map(label_map).fillna(0).astype(int)
    df_all['label_spender'] = df_all['permit_spender'].map(label_map).fillna(0).astype(int)
    df_all['label_relayer'] = df_all['relayer'].map(label_map).fillna(0).astype(int)
    df_all['label_tf_to'] = df_all['final_transfer_to'].map(label_map)

    is_phishing = (
        (df_all['label_submitter'] == 1) | (df_all['label_relayer'] == 1) |
        (df_all['label_spender'] == 1) | (df_all['label_tf_to'] == 1)
    )
    df_all['label'] = is_phishing.astype(int)

    # One-hot topology features
    df_all['feat_1_self_loop'] = (df_all['feat_topology'] == 'Self-Loop').astype(int)
    df_all['feat_2_kickback'] = (df_all['feat_topology'] == 'Kickback/Sweep').astype(int)
    df_all['feat_3_solver_settlement'] = (df_all['feat_topology'] == 'Solver-Settlement').astype(int)
    df_all['feat_4_self_rescue'] = (df_all['feat_topology'] == 'Self-Rescue').astype(int)
    df_all['feat_5_reflection'] = (df_all['feat_topology'] == 'Reflection').astype(int)
    df_all['feat_6_third_party'] = (df_all['feat_topology'] == 'Third-Party Leakage').astype(int)
    df_all.rename(columns={'feat_utilization': 'feat_7_utilization'}, inplace=True)

    # --- 8. Save ---
    final_cols = [
        'tx_hash', 'label', 'label_submitter', 'label_spender', 'label_tf_to',
        'permit_trace', 'transfer_trace', 'execution_status',
        'final_transfer_to', 'final_transfer_amount',
        'feat_1_self_loop', 'feat_2_kickback', 'feat_3_solver_settlement',
        'feat_4_self_rescue', 'feat_5_reflection', 'feat_6_third_party', 'feat_7_utilization',
    ]

    df_final = df_all[final_cols]
    output_path = PATHS["TRANSFER_FEATURES"]
    df_final.to_csv(output_path, index=False)

    num_phishing = df_final[df_final['label'] == 1].shape[0]
    print(f"Transfer features saved to {output_path}")
    print(f"  Total rows: {len(df_final)} | Phishing: {num_phishing} ({num_phishing / len(df_final):.2%})")


def analyze_transfer_result():
    """Compute descriptive statistics for transfer features grouped by label (benign vs phishing)."""
    result_path = PATHS["TRANSFER_FEATURES"]
    if not os.path.exists(result_path):
        print(f"File not found: {result_path}")
        return

    df = pd.read_csv(result_path)

    # Execution status distribution
    print("\n[1] Execution Status Distribution")
    status_pct = pd.crosstab(df['label'], df['execution_status'], normalize='index') * 100
    status_count = pd.crosstab(df['label'], df['execution_status'])

    for label_val, label_name in [(0, "Legitimate"), (1, "Phishing")]:
        print(f"\n  {label_name}:")
        if label_val in status_pct.index:
            for status in status_pct.loc[label_val].index:
                count = status_count.loc[label_val, status]
                pct = status_pct.loc[label_val, status]
                print(f"    {status:<15}: {count:>6} ({pct:.2f}%)")

    # Numerical feature stats
    print("\n[2] Numerical Feature Statistics")
    num_cols = ['feat_1_self_loop', 'feat_2_kickback', 'feat_3_solver_settlement',
                'feat_4_self_rescue', 'feat_5_reflection', 'feat_6_third_party', 'feat_7_utilization']

    stats = df.groupby('label')[num_cols].agg(['min', 'max', 'median', 'mean'])

    for label_val, label_name in [(0, "Legitimate"), (1, "Phishing")]:
        print(f"\n  {label_name}:")
        if label_val not in stats.index:
            continue
        print(f"  {'Feature':<25} | {'Min':<8} | {'Max':<8} | {'Median':<8} | {'Mean':<12}")
        print(f"  {'-' * 70}")
        for feat in num_cols:
            _min = stats.loc[label_val, (feat, 'min')]
            _max = stats.loc[label_val, (feat, 'max')]
            _med = stats.loc[label_val, (feat, 'median')]
            _mean = stats.loc[label_val, (feat, 'mean')]
            mean_str = f"{_mean:.4f}" if "utilization" in feat else f"{_mean * 100:.2f}%"
            print(f"  {feat:<25} | {_min:<8.2f} | {_max:<8.2f} | {_med:<8.2f} | {mean_str:<12}")


if __name__ == '__main__':
    run_transfer_analysis()
    # analyze_transfer_result()
