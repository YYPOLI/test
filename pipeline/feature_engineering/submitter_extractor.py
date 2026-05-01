"""
Submitter feature extraction: compute 10 behavioral dimensions per original_submitter
by aggregating all permit transactions associated with that submitter.
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


def safe_to_int(x):
    if pd.isna(x) or str(x).strip() == '':
        return 0
    s = str(x).strip()
    try:
        if s.lower().startswith('0x'):
            return int(s, 16)
        return int(s)
    except ValueError:
        try:
            return int(float(s))
        except Exception:
            return 0
    except Exception:
        return 0


def run_submitter_analysis():
    print("Starting Submitter feature extraction...")

    # --- 1. Load knowledge bases ---
    label_path = PATHS["ADDRESS_LABELS"]
    df_labels = pd.read_csv(label_path, usecols=['address', 'label', 'nametag'], encoding_errors='replace')
    df_labels['address'] = df_labels['address'].str.lower().str.strip()
    label_map = df_labels.set_index('address')['label'].to_dict()
    name_map = df_labels.set_index('address')['nametag'].to_dict()

    token_path = PATHS["TOKEN_KB"]
    df_token_1000 = pd.read_csv(token_path, encoding_errors='replace')
    df_token_1000['contract_address'] = df_token_1000['contract_address'].str.lower().str.strip()
    decimal_map = df_token_1000.set_index('contract_address')['decimals'].to_dict()

    spender_path = PATHS["SPENDER_FEATURES"]
    df_spender = pd.read_csv(spender_path, encoding_errors='replace')
    df_spender['address'] = df_spender['address'].str.lower().str.strip()
    ghost_map = df_spender.set_index('address')['is_ghost'].to_dict()

    # --- 2. Load permit transaction data ---
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
    col_to_str = ['original_submitter', 'relayer', 'token_address', 'permit_owner',
                  'permit_spender', 'transfer_from', 'transfer_to']
    for col in col_to_str:
        df_all[col] = df_all[col].str.lower().str.strip()

    df_all['permit_deadline'] = df_all['permit_deadline'].apply(safe_to_int)
    df_all['timestamp'] = pd.to_datetime(df_all['timestamp']).view('int64') // 10 ** 9

    df_all['final_transfer_to'] = 'none'
    df_all['final_transfer_amount'] = 0.0
    df_all['execution_status'] = 'Unused_Dormant'

    SPENDER_HISTORY_PATH = os.path.join(BASE_PATH, "verified_permit/labeled_address/1_order_0609/")
    TRANSFER_FROM_METHOD = "0x23b872dd"

    # --- 4. Resolve atomic & delayed transfers ---
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

    # Delayed drain backtracking
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
            permit_ts = row['timestamp']
            victim = row['permit_owner'].replace("0x", "")

            for _, tx in df_hist[df_hist['timeStamp'] > permit_ts].iterrows():
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

    # --- 5. Label mapping ---
    df_all['label_submitter'] = df_all['original_submitter'].map(label_map)
    df_all['label_relayer'] = df_all['relayer'].map(label_map)
    df_all['label_spender'] = df_all['permit_spender'].map(label_map)
    df_all['label_tf_to'] = df_all['final_transfer_to'].map(label_map)

    df_all['decimals'] = df_all['token_address'].map(decimal_map).fillna(18).astype(int)
    df_all['is_infinite_value'] = df_all['permit_value'].apply(lambda x: float(x) > 1e50)
    df_all['is_infinite_deadline'] = df_all['permit_deadline'].apply(lambda x: x > 1e14)

    # --- 6. Per-row feature computation ---
    df_all['feat_direct_call'] = (df_all['original_submitter'] == df_all['relayer'])
    df_all['feat_self_submit'] = (df_all['original_submitter'] == df_all['permit_owner'])
    df_all['feat_ghost_spender'] = df_all['permit_spender'].map(ghost_map)
    df_all['feat_mediated_transfer'] = (
        (df_all['final_transfer_to'] != df_all['original_submitter']) &
        (df_all['final_transfer_to'] != df_all['relayer']) &
        (df_all['final_transfer_to'] != df_all['permit_owner']) &
        (df_all['final_transfer_to'] != df_all['permit_spender']) &
        (df_all['final_transfer_to'] != df_all['token_address'])
    )
    df_all['feat_lp_token'] = (
        df_all['feat_self_submit'] &
        (df_all['relayer'] == df_all['permit_spender']) &
        (df_all['final_transfer_to'] == df_all['token_address'])
    ).astype(int)
    df_all['feat_atomic'] = df_all['is_atomic'].astype(bool)
    df_all['feat_high_value'] = df_all['token_address'].isin(df_token_1000['contract_address'])
    df_all['feat_infinite_value'] = df_all['is_infinite_value']
    df_all['feat_infinite_deadline'] = df_all['is_infinite_deadline']

    # --- 7. Aggregation by submitter ---
    df_all['target_lp_token'] = df_all['token_address'].where(df_all['feat_lp_token'] == 1)

    agg_funcs = {
        'tx_hash': 'count', 'token_address': 'nunique', 'permit_owner': 'nunique',
        'permit_spender': 'nunique', 'feat_direct_call': 'sum', 'feat_self_submit': 'sum',
        'feat_ghost_spender': 'sum', 'feat_mediated_transfer': 'sum', 'feat_atomic': 'sum',
        'feat_high_value': 'sum', 'feat_infinite_value': 'sum', 'feat_infinite_deadline': 'sum',
        'feat_lp_token': 'sum', 'target_lp_token': 'nunique',
    }

    df_stats = df_all.groupby(['original_submitter', 'label_submitter']).agg(agg_funcs)
    df_stats.rename(columns={
        'tx_hash': 'total_txs', 'token_address': 'unique_tokens',
        'permit_owner': 'unique_victims', 'permit_spender': 'unique_spenders',
        'target_lp_token': 'unique_lp_tokens',
    }, inplace=True)

    df_stats['ratio_direct_call'] = df_stats['feat_direct_call'] / df_stats['total_txs']
    df_stats['ratio_self_submit'] = df_stats['feat_self_submit'] / df_stats['total_txs']
    df_stats['ratio_ghost'] = df_stats['feat_ghost_spender'] / df_stats['total_txs']
    df_stats['ratio_mediated'] = df_stats['feat_mediated_transfer'] / df_stats['total_txs']
    df_stats['ratio_atomic'] = df_stats['feat_atomic'] / df_stats['total_txs']
    df_stats['ratio_high_value'] = df_stats['feat_high_value'] / df_stats['total_txs']
    df_stats['ratio_infinite_value'] = df_stats['feat_infinite_value'] / df_stats['total_txs']
    df_stats['ratio_infinite_deadline'] = df_stats['feat_infinite_deadline'] / df_stats['total_txs']
    df_stats['spender_fidelity_score'] = df_stats['total_txs'] / df_stats['unique_spenders']

    df_stats = df_stats.reset_index()

    final_cols = [
        'original_submitter', 'label_submitter', 'total_txs', 'unique_tokens',
        'unique_victims', 'unique_spenders', 'spender_fidelity_score',
        'ratio_direct_call', 'ratio_self_submit', 'ratio_ghost', 'ratio_mediated',
        'ratio_atomic', 'ratio_high_value', 'ratio_infinite_value', 'ratio_infinite_deadline',
        'feat_lp_token', 'unique_lp_tokens',
    ]

    df_final = df_stats[final_cols]
    output_path = PATHS["SUBMITTER_FEATURES"]
    df_final.to_csv(output_path, index=False)
    print(f"Submitter features saved to {output_path}")


def analyze_submitter_results():
    """Compute descriptive statistics (min/max/median/mean) for submitter features, grouped by label."""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.float_format', '{:.4f}'.format)

    result_path = PATHS["SUBMITTER_FEATURES"]
    df_result = pd.read_csv(result_path)

    print(f"Total samples: {len(df_result)}")
    print(f"Label distribution:\n{df_result['label_submitter'].value_counts()}")

    numeric_cols = [
        'total_txs', 'unique_tokens', 'unique_victims', 'unique_spenders',
        'spender_fidelity_score', 'ratio_direct_call', 'ratio_self_submit',
        'ratio_ghost', 'ratio_mediated', 'ratio_atomic', 'ratio_high_value',
        'ratio_infinite_value', 'ratio_infinite_deadline', 'feat_lp_token', 'unique_lp_tokens',
    ]

    for col in numeric_cols:
        if col in df_result.columns:
            df_result[col] = pd.to_numeric(df_result[col], errors='coerce')

    stats = df_result.groupby('label_submitter')[numeric_cols].agg(['min', 'max', 'median', 'mean'])

    for lbl in df_result['label_submitter'].unique():
        print(f"\nLabel: {lbl}")
        print("~" * 80)
        subset = stats.loc[lbl]
        summary_df = subset.unstack(level=1)[['min', 'max', 'median', 'mean']]
        print(summary_df)

    # LP Token distribution
    df_lp = df_result[(df_result['feat_lp_token'] != 0) & (df_result['unique_lp_tokens'] != 0)].copy()
    bins = [0, 1, 2, 3, 4, 5, float('inf')]
    labels_bins = ['1', '2', '3', '4', '5', '>5']

    for col in ['feat_lp_token', 'unique_lp_tokens']:
        if col in df_lp.columns:
            print(f"\nFeature: {col}")
            bucket_col = f'{col}_bucket'
            df_lp[bucket_col] = pd.cut(df_lp[col], bins=bins, labels=labels_bins)
            dist_df = df_lp.groupby(['label_submitter', bucket_col], observed=False).size().unstack(fill_value=0)
            dist_pct = dist_df.div(dist_df.sum(axis=1), axis=0) * 100
            print("Count Distribution:")
            print(dist_df)
            print("\nPercentage Distribution (%):")
            print(dist_pct.round(2))


if __name__ == '__main__':
    run_submitter_analysis()
    # analyze_submitter_results()
