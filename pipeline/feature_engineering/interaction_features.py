"""
Interaction (cross-stage) feature computation:
Combines entity-level profiles with per-transaction flags to produce
the 9 composite binary features used by the Semantic Aligner.
"""

import os
import re
import sys

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.utils.config import CONFIG
from pipeline.data_processing.data_processor import get_file_list

PATHS = CONFIG["PATHS"]


def _load_knowledge_base():
    """Load pre-computed Spender, Submitter, Transfer, and Token knowledge bases."""
    sp_df = pd.read_csv(PATHS["SPENDER_FEATURES"])
    sp_df['address'] = sp_df['address'].str.lower().str.strip()
    sp_map = sp_df.set_index('address').to_dict(orient='index')

    sb_df = pd.read_csv(PATHS["SUBMITTER_FEATURES"])
    sb_df['original_submitter'] = sb_df['original_submitter'].str.lower().str.strip()
    sb_map = sb_df.set_index('original_submitter').to_dict(orient='index')

    tf_df = pd.read_csv(PATHS["TRANSFER_FEATURES"])
    tf_df['tx_hash'] = tf_df['tx_hash'].str.lower().str.strip()

    tk_df = pd.read_csv(PATHS["TOKEN_KB"])
    tk_df['contract_address'] = tk_df['contract_address'].str.lower().str.strip()

    return sp_map, sb_map, tf_df, tk_df


def compute_interaction_features():
    """Compute 9 composite binary features by merging entity profiles with transaction data."""
    print("Starting interaction feature computation...")

    # 1. Load permit transactions
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

    for col in ['permit_spender', 'original_submitter', 'permit_owner', 'transfer_to']:
        if col in df_all.columns:
            df_all[col] = df_all[col].str.lower().str.strip()

    df_all['timestamp'] = pd.to_datetime(df_all['timestamp'], errors='coerce', utc=True).astype('int64') // 10 ** 9

    # 2. Load knowledge bases
    sp_map, sb_map, tf_df, tk_df = _load_knowledge_base()

    # 3. Merge transfer features
    if not tf_df.empty:
        print("  Merging transfer features...")
        df_all = pd.merge(df_all, tf_df, on=['tx_hash', 'permit_trace', 'transfer_trace'], how='left')
        df_all['execution_status'] = df_all['execution_status'].fillna('Unknown')
        df_all['feat_7_utilization'] = df_all['feat_7_utilization'].fillna(0.0)
    else:
        df_all['execution_status'] = 'Unknown'
        df_all['feat_7_utilization'] = 0.0

    # 4. Map entity profiles
    df_all['sp_label'] = df_all['permit_spender'].apply(lambda x: sp_map.get(x, {}).get('label_spender', 0))
    df_all['sp_is_ghost'] = df_all['permit_spender'].apply(lambda x: sp_map.get(x, {}).get('is_ghost', True))
    df_all['sp_lifespan'] = df_all['permit_spender'].apply(lambda x: sp_map.get(x, {}).get('lifespan_hours', 0))
    df_all['sp_fidelity'] = df_all['original_submitter'].apply(lambda x: sb_map.get(x, {}).get('spender_fidelity_score', 0))
    df_all['sb_tx_count'] = df_all['original_submitter'].apply(lambda x: sb_map.get(x, {}).get('total_txs', 1))
    df_all['top_token'] = df_all['token_address'].isin(tk_df['contract_address'])

    # 5. Compute base flags
    df_all['flag_inf_amt'] = pd.to_numeric(df_all['permit_value'], errors='coerce') > 1e50
    df_all['flag_inf_time'] = (pd.to_numeric(df_all['permit_deadline'], errors='coerce') - df_all['timestamp']) > 31536000
    df_all['flag_self_submit'] = df_all['original_submitter'] == df_all['permit_owner']
    df_all['flag_relayer_spender'] = df_all['relayer'] == df_all['permit_spender']

    # 6. Compute 9 composite features
    print("  Computing composite features...")

    df_all['feat_unverified_infinite'] = (df_all['flag_inf_amt'] & df_all['sp_is_ghost']).astype(int)
    df_all['feat_time_reputation_div'] = (df_all['flag_inf_time'] & (df_all['sp_lifespan'] < 24)).astype(int)
    df_all['feat_sovereignty_leakage'] = (df_all['flag_self_submit'] & df_all['feat_6_third_party']).astype(int)
    df_all['feat_relayed_theft'] = ((~df_all['flag_self_submit']) & df_all['feat_6_third_party']).astype(int)
    df_all['feat_porter_anomaly'] = ((df_all['sb_tx_count'] > 10) & (df_all['sp_fidelity'] > 5)).astype(int)
    df_all['feat_signature_harvesting'] = (df_all['flag_inf_amt'] & (df_all['feat_7_utilization'] < 0.001)).astype(int)
    df_all['feat_dormant_risk'] = (df_all['flag_inf_time'] & (df_all['execution_status'] == 'Unused_Dormant')).astype(int)
    df_all['feat_lp_token'] = (df_all['flag_self_submit'] & df_all['flag_relayer_spender'] & df_all['feat_5_reflection']).astype(int)
    df_all['feat_strange'] = (df_all['flag_self_submit'] & df_all['flag_relayer_spender'] & df_all['feat_6_third_party']).astype(int)

    # 7. Save
    cols_to_save = [
        'tx_hash', 'label', 'timestamp', 'permit_trace', 'transfer_trace',
        'execution_status', 'feat_7_utilization',
        'feat_unverified_infinite', 'feat_time_reputation_div',
        'feat_sovereignty_leakage', 'feat_relayed_theft', 'feat_porter_anomaly',
        'feat_signature_harvesting', 'feat_dormant_risk', 'feat_lp_token', 'feat_strange',
    ]
    cols_to_save = [c for c in cols_to_save if c in df_all.columns]

    out_file = PATHS["COMBINED_FEATURES"]
    df_all[cols_to_save].to_csv(out_file, index=False)
    print(f"Interaction features saved to {out_file}")


def analyze_interaction_features():
    """Analyze composite feature hit rates and risk multipliers between benign and phishing."""
    FEATURE_PATH = PATHS["COMBINED_FEATURES"]

    if not os.path.exists(FEATURE_PATH):
        print(f"File not found: {FEATURE_PATH}")
        return

    df_feat = pd.read_csv(FEATURE_PATH)

    binary_cols = [
        'feat_unverified_infinite', 'feat_time_reputation_div',
        'feat_sovereignty_leakage', 'feat_relayed_theft', 'feat_porter_anomaly',
        'feat_signature_harvesting', 'feat_dormant_risk', 'feat_lp_token', 'feat_strange',
    ]

    print("\n[1] Risk Feature Hit Rate & Multiplier")
    print(f"{'-' * 100}")
    print(f"{'Feature':<30} | {'Benign':<12} | {'Phishing':<12} | {'Multiplier':<15} | {'Diff':<10}")
    print(f"{'-' * 100}")

    stats = df_feat.groupby('label')[binary_cols].mean()

    for col in binary_cols:
        rate_0 = stats.loc[0, col] if 0 in stats.index else 0
        rate_1 = stats.loc[1, col] if 1 in stats.index else 0
        multiplier = rate_1 / rate_0 if rate_0 > 0 else (np.inf if rate_1 > 0 else 0)
        diff = rate_1 - rate_0
        mult_str = f"{multiplier:.1f}x" if multiplier != np.inf else "Infinite"
        print(f"{col:<30} | {rate_0:<12.2%} | {rate_1:<12.2%} | {mult_str:<15} | +{diff:<.2%}")

    # Execution status distribution
    print(f"\n[2] Execution Status Distribution")
    status_cross = pd.crosstab(df_feat['label'], df_feat['execution_status'], normalize='index') * 100
    status_count = pd.crosstab(df_feat['label'], df_feat['execution_status'])

    for label_val, label_name in [(0, "Legitimate"), (1, "Phishing")]:
        print(f"\n  {label_name}:")
        if label_val in status_cross.index:
            for status in status_cross.loc[label_val].index:
                count = status_count.loc[label_val, status]
                pct = status_cross.loc[label_val, status]
                bar = "█" * int(pct // 5)
                print(f"    {status:<15}: {count:<6}  {pct:>6.2f}%  {bar}")


if __name__ == '__main__':
    compute_interaction_features()
    # analyze_interaction_features()
