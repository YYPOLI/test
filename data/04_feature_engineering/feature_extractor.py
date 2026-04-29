"""
Stage 4: Feature engineering for Spender / Submitter / Transfer entity profiling.
Extracts behavioral features from historical transaction data for each entity.
"""

import os
import sys
import warnings

import pandas as pd
import numpy as np
from tqdm import tqdm

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.utils.hex_parser import HexParser
from src.utils.config import CONFIG

BASE_PATH = CONFIG["BASE_PATH"]
BQ_PATH = os.path.join(BASE_PATH, "BigQuery_since20251230/new_data_0117")


def safe_to_int(x):
    """Convert various formats (hex, float string, scientific notation) to Python int."""
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


def load_token_metadata():
    """Load token metadata: decimals, price, symbol from Top 1000 tokens CSV."""
    path = os.path.join(BASE_PATH, "verified_permit/labeled_address/features/Top_1000_Tokens_Decimals.csv")
    if not os.path.exists(path):
        print("Error: token metadata file not found.")
        return {}
    df = pd.read_csv(path)
    df['contract_address'] = df['contract_address'].astype(str).str.lower().str.strip()
    token_map = {}
    for _, row in df.iterrows():
        token_map[row['contract_address']] = {
            'decimals': row['decimals'],
            'price': row['current_price'],
            'symbol': row['symbol'],
            'name': row['name'],
            'mcap': row['market_cap'],
        }
    print(f"Loaded {len(token_map)} token metadata entries.")
    return token_map


def extract_spender_features(address, label, name_tag, address_type, top_token, super_large_address):
    """
    Compute 5-dimensional behavioral features for a single Spender address
    by reading its external/internal/token transaction history files.
    """
    DATA_ROOT = os.path.join(BASE_PATH, "verified_permit/labeled_address/1_order_0609/")
    folder_path = os.path.join(DATA_ROOT, address)
    is_super_large = (address in super_large_address)

    f_ex = os.path.join(folder_path, f"{address}_external.csv")
    f_in = os.path.join(folder_path, f"{address}_internal.csv")
    f_tk = os.path.join(folder_path, f"{address}_tokentx.csv")

    base_result = {
        'address': address, 'label': label, 'name_tag': name_tag, 'type': address_type,
    }

    if is_super_large:
        return {**base_result, 'total_tx_count': -1, 'lifespan_hours': -1, 'tx_density': -1,
                'mediated_theft_count': -1, 'direct_in_count': -1, 'mediated_op_ratio': -1,
                'token_flow_ratio': -1, 'sender_receiver_ratio': -1, 'is_ghost': 0,
                'top1_gas_payer_ratio': -1, 'high_value_ratio': -1}

    df_ex, df_in, df_token = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    try:
        if os.path.exists(f_ex): df_ex = pd.read_csv(f_ex, dtype=str)
        if os.path.exists(f_in): df_in = pd.read_csv(f_in, dtype=str)
        if os.path.exists(f_tk): df_token = pd.read_csv(f_tk, dtype=str)
    except Exception:
        return {**base_result, 'total_tx_count': 0, 'lifespan_hours': 0, 'tx_density': 0,
                'mediated_theft_count': 0, 'direct_in_count': 0, 'mediated_op_ratio': 0,
                'token_flow_ratio': 0, 'sender_receiver_ratio': 0, 'is_ghost': 1,
                'top1_gas_payer_ratio': 0, 'high_value_ratio': 0}

    timestamps = []
    if not df_ex.empty: timestamps.extend(df_ex['timeStamp'].astype(float).tolist())
    if not df_in.empty: timestamps.extend(df_in['timeStamp'].astype(float).tolist())
    if not df_token.empty: timestamps.extend(df_token['timeStamp'].astype(float).tolist())

    total_tx_count = len(df_ex) + len(df_in) + len(df_token)

    lifespan_hours, tx_density = 0, 0
    if timestamps:
        lifespan_seconds = max(timestamps) - min(timestamps)
        lifespan_hours = lifespan_seconds / 3600
        tx_density = total_tx_count / lifespan_hours if lifespan_hours > 0 else total_tx_count

    # --- Fund flow analysis ---
    mediated_flows = []
    tf_candidates = pd.DataFrame()
    if not df_ex.empty:
        tf_candidates = df_ex[
            (df_ex['isError'] == '0') & (df_ex['input'].str.startswith('0x23b872dd'))
        ]
        for _, row in tf_candidates.iterrows():
            tx_info = HexParser.parse_transfer(row['input'])
            if tx_info:
                mediated_flows.append(tx_info)

    df_mediated = pd.DataFrame(mediated_flows, columns=['tf_from', 'tf_to', 'tf_value']) if mediated_flows else pd.DataFrame()

    in_token_count = (df_token['to'].str.lower() == address).sum() if not df_token.empty else 0
    out_token_count = (df_token['from'].str.lower() == address).sum() if not df_token.empty else 0

    mediated_theft_count = 0
    if not df_mediated.empty:
        mediated_theft_count = len(df_mediated[df_mediated['tf_to'].str.lower() != address.lower()])

    direct_in_count = in_token_count
    if not df_mediated.empty:
        direct_in_count += (df_mediated['tf_to'].str.lower() == address.lower()).sum()

    mediated_op_ratio = mediated_theft_count / len(tf_candidates) if len(tf_candidates) > 0 else 0
    token_flow_ratio = out_token_count / in_token_count if in_token_count > 0 else 0
    sender_receiver_ratio = len(df_token['from'].unique()) / len(df_token['to'].unique()) if not df_token.empty and len(df_token['to'].unique()) > 0 else 0

    is_ghost = 1 if ((len(df_ex) == 0 and len(df_in) > 0) or total_tx_count == 0) else 0

    top1_gas_payer_ratio = 0
    if not df_ex.empty:
        payers = df_ex['from'].value_counts(normalize=True)
        if not payers.empty:
            top1_gas_payer_ratio = payers.iloc[0]

    high_value_ratio = 0
    if not df_token.empty:
        valid_tokens = df_token['contractAddress'].str.lower().isin(top_token['contract_address'])
        high_value_ratio = valid_tokens.sum() / len(df_token)

    return {
        **base_result,
        'total_tx_count': total_tx_count, 'lifespan_hours': lifespan_hours,
        'tx_density': tx_density, 'mediated_theft_count': mediated_theft_count,
        'direct_in_count': direct_in_count, 'mediated_op_ratio': mediated_op_ratio,
        'token_flow_ratio': token_flow_ratio, 'sender_receiver_ratio': sender_receiver_ratio,
        'is_ghost': is_ghost, 'top1_gas_payer_ratio': top1_gas_payer_ratio,
        'high_value_ratio': high_value_ratio,
    }


def run_spender_analysis():
    """Main entry point for Spender feature extraction."""
    print("Starting Spender feature extraction...")

    label_path = os.path.join(BQ_PATH, "cleaned_data/labeled_address/all_labeled_address_883071.csv")
    df_labels = pd.read_csv(label_path, usecols=['address', 'label', 'nametag'], encoding_errors='replace')
    df_labels['address'] = df_labels['address'].str.lower().str.strip()
    label_map = df_labels.set_index('address')['label'].to_dict()
    name_map = df_labels.set_index('address')['nametag'].to_dict()

    token_path = os.path.join(BASE_PATH, "verified_permit/labeled_address/features/Top_1000_Tokens_Decimals.csv")
    df_token_1000 = pd.read_csv(token_path, encoding_errors='replace')
    df_token_1000['contract_address'] = df_token_1000['contract_address'].str.lower().str.strip()

    large_path = os.path.join(BASE_PATH, "verified_permit/labeled_address/large_address_0124.csv")
    df_large = pd.read_csv(large_path, encoding_errors='replace')
    df_large['address'] = df_large['address'].str.lower().str.strip()
    super_large_set = set(df_large[df_large['super_large'] == True]['address'])

    spender_path = os.path.join(BQ_PATH, "cleaned_data/train_data/all_spender_address_21months_9528.csv")
    df_spender = pd.read_csv(spender_path, usecols=['address', 'address_type'], encoding_errors='replace')
    df_spender['address'] = df_spender['address'].str.lower().str.strip()
    df_spender['label_spender'] = df_spender['address'].map(label_map)
    df_spender['nametag_spender'] = df_spender['address'].map(name_map)

    results = []
    for _, row in tqdm(df_spender.iterrows(), total=len(df_spender)):
        feat = extract_spender_features(
            row['address'], row['label_spender'], row['nametag_spender'],
            row['address_type'], df_token_1000, super_large_set,
        )
        results.append(feat)

    df_res = pd.DataFrame(results)
    output_path = os.path.join(BASE_PATH, "BigQuery_since20251230/analysis/spender_analysis_result.csv")
    df_res.to_csv(output_path, index=False)
    print(f"Spender features saved to {output_path}")


def analyze_spender_results():
    """Compute descriptive statistics for spender features, grouped by label."""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.float_format', '{:.4f}'.format)

    result_path = os.path.join(BASE_PATH, "BigQuery_since20251230/analysis/spender_analysis_result_21months_0209.csv")
    df_result = pd.read_csv(result_path)

    print(f"Total samples: {len(df_result)}")
    print(f"Label distribution:\n{df_result['label_spender'].value_counts()}")

    # Address type distribution
    print("\n[1] Address Type Distribution")
    type_dist = pd.crosstab(df_result['label_spender'], df_result['type'], normalize='index') * 100
    print(type_dist.round(2).astype(str) + '%')

    # Non-ghost ratio
    print("\n[2] Non-Ghost Ratio")
    ghost_stats = df_result.groupby('label_spender')['is_ghost'].apply(lambda x: (x == 0).mean() * 100)
    ghost_df = ghost_stats.to_frame(name='Non-Ghost Ratio (%)')
    print(ghost_df.round(2).astype(str) + '%')

    # Core feature statistics (exclude truncated large addresses)
    print("\n[3] Core Feature Statistics (excluding truncated)")
    df_valid = df_result[(df_result['total_tx_count'] != -1) & (df_result['is_ghost'] == 0)].copy()

    numeric_cols = [
        'total_tx_count', 'lifespan_hours', 'tx_density',
        'mediated_theft_count', 'direct_in_count', 'mediated_op_ratio',
        'token_flow_ratio', 'sender_receiver_ratio', 'top1_gas_payer_ratio', 'high_value_ratio',
    ]

    for col in numeric_cols:
        if col in df_valid.columns:
            df_valid[col] = pd.to_numeric(df_valid[col], errors='coerce')

    stats = df_valid.groupby('label_spender')[numeric_cols].agg(['min', 'max', 'median', 'mean'])

    for lbl in df_valid['label_spender'].unique():
        print(f"\nLabel: {lbl}")
        print("~" * 80)
        subset = stats.loc[lbl]
        summary_df = subset.unstack(level=1)[['min', 'max', 'median', 'mean']]
        print(summary_df)


if __name__ == '__main__':
    run_spender_analysis()
    # analyze_spender_results()
