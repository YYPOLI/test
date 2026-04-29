"""
PermitGuard end-to-end detection pipeline.
Loads the test dataset, enriches each transaction, runs the LLM auditor,
and computes classification metrics.
"""

import sys
import os
import json
import time
import warnings

import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils.config import CONFIG
from src.permit_parser.context_retriever import KnowledgeBase
from src.permit_parser.payload_decoder import DeepFactEnricher
from src.cognitive_reasoner.constrained_inferencer import PermitGuardAuditor

warnings.filterwarnings('ignore')


def main():
    main_start_time = time.perf_counter()
    print("Starting PermitGuard detection pipeline...")

    start_time_1 = time.perf_counter()
    kb = KnowledgeBase()

    try:
        df = pd.read_csv(CONFIG["PATHS"]["PARSED_DATASET"])
        df = df.iloc[0:1000]
        print(f"Loaded {len(df)} transactions for audit.")
    except Exception as e:
        print(f"Data loading failed: {e}")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp']).view('int64') // 10 ** 9
    df['permit_value'] = df['permit_value'].astype(float)
    df['transfer_amount_hex'] = df['transfer_amount_hex'].astype(float)
    df['submitter_label'] = df['original_submitter'].map(kb.label_map)
    df['submitter_nametag'] = df['original_submitter'].map(kb.name_map)
    df['relayer_label'] = df['relayer'].map(kb.label_map)
    df['relayer_nametag'] = df['relayer'].map(kb.name_map)
    df['spender_label'] = df['permit_spender'].map(kb.label_map)
    df['spender_nametag'] = df['permit_spender'].map(kb.name_map)
    df['transfer_to_label'] = df['transfer_to'].map(kb.label_map)
    df['transfer_to_nametag'] = df['transfer_to'].map(kb.name_map)
    df['transfer_to_label'] = df['transfer_to_label'].fillna(0).astype(int)

    enricher = DeepFactEnricher()
    auditor = PermitGuardAuditor()
    results = []
    total = len(df)

    end_time_1 = time.perf_counter()
    data_time = end_time_1 - start_time_1
    print(f"Data loading time: {data_time:.2f}s")

    for idx, row in df.iterrows():
        start_time_2 = time.perf_counter()
        sys.stdout.write(f"\r[Progress] {idx + 1}/{total} ({(idx + 1) / total * 100:.1f}%)")
        sys.stdout.flush()

        facts = enricher.enrich(row)
        end_time_2 = time.perf_counter()
        enrich_time = end_time_2 - start_time_2

        start_time_3 = time.perf_counter()
        if row['submitter_label'] == 2 and row['relayer_label'] == 2 and row['spender_label'] == 2 and row['transfer_to_label'] == 2:
            audit_result = {
                "risk_level": "LEGITIMATE",
                "confidence_score": "100%",
                "conclusion": "Whitelisted entity",
                "usage": {"prompt_tokens": 0, "completion_tokens": 0},
            }
        elif row['permit_value'] == 0:
            audit_result = {
                "risk_level": "LEGITIMATE",
                "confidence_score": "100%",
                "conclusion": "Revocation",
                "usage": {"prompt_tokens": 0, "completion_tokens": 0},
            }
        else:
            audit_result = auditor.audit(facts)

        end_time_3 = time.perf_counter()
        reasoning_time = end_time_3 - start_time_3

        usage_data = audit_result.pop('usage', {"prompt_tokens": 0, "completion_tokens": 0})

        final_record = {
            "Number": idx + 1,
            "tx_hash": row['tx_hash'],
            "block_number": row['block_number'],
            "timestamp": row["timestamp"],
            "submitter": row["original_submitter"],
            "submitter_nametag": row["submitter_nametag"],
            "relayer": row["relayer"],
            "relayer_nametag": row['relayer_nametag'],
            "token_address": row["token_address"],
            "token_symbol": facts["permit_intent"]["token_symbol"],
            "owner": row["permit_owner"],
            "spender": row["permit_spender"],
            "spender_nametag": row["spender_nametag"],
            "permit_raw_value": row["permit_value"],
            "permit_usd_value": facts['permit_intent']['permit_usd_value'],
            "validity_period": facts['permit_intent']['validity_period'],
            "execution_status": facts["execution_forensics"]["execution_status"],
            "transfer_to": facts["execution_forensics"]["transfer_to"],
            "transfer_to_nametag": row["transfer_to_nametag"],
            "transfer_raw_value": facts["execution_forensics"]["transfer_raw_value"],
            "transfer_usd_value": facts["execution_forensics"]["transfer_usd_value"],
            "permit_transfer_ratio": facts["execution_forensics"]["permit_transfer_ratio"],
            "transfer_type": facts["execution_forensics"]["transfer_type"],
            "ground_truth_label": row["ground_truth_label"],
            "ai_analysis": audit_result,
            "prompt_tokens": usage_data.get('prompt_tokens', 0),
            "completion_tokens": usage_data.get('completion_tokens', 0),
            "total_tokens": usage_data.get('prompt_tokens', 0) + usage_data.get('completion_tokens', 0),
            "data_time": data_time / total,
            "enrich_time": enrich_time,
            "reasoning_time": reasoning_time,
        }
        results.append(final_record)

        if (idx + 1) % 5 == 0:
            with open(CONFIG["PATHS"]["OUTPUT_REPORT"], 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\nSaving report to {CONFIG['PATHS']['OUTPUT_REPORT']}")
    with open(CONFIG["PATHS"]["OUTPUT_REPORT"], 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # --- Metrics ---
    y_true, y_pred = [], []
    for res in results:
        y_true.append(res['ground_truth_label'])
        y_pred.append(1 if res['ai_analysis']['risk_level'] in ['CRITICAL'] else 0)

    try:
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
        fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

        print("\n" + "=" * 40)
        print("     PermitGuard Performance Report     ")
        print("=" * 40)
        print(f"Samples  : {len(y_true)}")
        print(f"F1-Score : {f1:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall   : {recall:.4f}")
        print(f"FPR      : {fpr:.4f}")
        print(f"TP: {tp} | FP: {fp} | FN: {fn} | TN: {tn}")
        print("=" * 40)
    except Exception as e:
        print(f"[Eval Error] {e}")

    print(f"Total time: {time.perf_counter() - main_start_time:.2f}s")


if __name__ == "__main__":
    main()
