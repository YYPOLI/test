"""
Post-hoc metric calculation from a saved PermitGuard audit report JSON.
Also exports FP / FN cases for manual review.
"""

import sys
import os
import json

import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils.config import CONFIG


def calculate_result():
    df = pd.read_json(CONFIG["PATHS"]["OUTPUT_REPORT"])

    def extract_risk(ai_analysis):
        if isinstance(ai_analysis, dict):
            return ai_analysis.get('risk_level', 'UNKNOWN')
        return 'UNKNOWN'

    df['extracted_risk_level'] = df['ai_analysis'].apply(extract_risk)

    thresholds = ['CRITICAL']
    df['pred_label'] = df['extracted_risk_level'].apply(lambda x: 1 if x in thresholds else 0)

    gt_col = 'ground_truth' if 'ground_truth' in df.columns else 'ground_truth_label'
    if gt_col not in df.columns:
        print("Error: missing ground_truth label column")
        return

    df['ground_truth'] = df[gt_col].astype(int)

    try:
        y_true = df['ground_truth']
        y_pred = df['pred_label']

        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
        fpr = fp / (fp + tn) if (fp + tn) > 0 else 0.0

        print(f"Samples  : {len(df)}")
        print("-" * 40)
        print(f"F1-Score : {f1:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall   : {recall:.4f}")
        print(f"FPR      : {fpr:.4f}")
        print("-" * 40)
        print(f"TP: {tp} | FP: {fp} | FN: {fn} | TN: {tn}")

    except Exception as e:
        print(f"[Eval Error] {e}")
        return

    df_fp = df[(df['ground_truth'] == 0) & (df['pred_label'] == 1)]
    df_fn = df[(df['ground_truth'] == 1) & (df['pred_label'] == 0)]

    print("\n" + "=" * 40)
    if not df_fp.empty:
        fp_path = CONFIG["PATHS"]["FP_OUTPUT"]
        df_fp.to_json(fp_path, orient='records', force_ascii=False, indent=2)
        print(f"FP cases: {len(df_fp)} -> {fp_path}")
    else:
        print("No FP cases found.")

    if not df_fn.empty:
        fn_path = CONFIG["PATHS"]["FN_OUTPUT"]
        df_fn.to_json(fn_path, orient='records', force_ascii=False, indent=2)
        print(f"FN cases: {len(df_fn)} -> {fn_path}")
    else:
        print("No FN cases found.")


if __name__ == "__main__":
    calculate_result()
