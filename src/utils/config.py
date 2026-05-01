import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
PIPELINE_OUTPUT_DIR = os.path.join(DATA_DIR, "pipeline_output")
RESULT_DIR = os.path.join(PROJECT_ROOT, "result")

BASE_DATA_PATH = os.getenv("BASE_DATA_PATH", "")

def _resolve(data_relative: str, full_path: str) -> str:
    """Use full external path if BASE_DATA_PATH is set, otherwise fall back to data/."""
    if BASE_DATA_PATH:
        return os.path.join(BASE_DATA_PATH, full_path)
    return os.path.join(DATA_DIR, data_relative)

CONFIG = {
    "GEMINI_API_KEY": os.getenv("GEMINI_API_KEY", ""),
    "QWEN_API_KEY": os.getenv("QWEN_API_KEY", ""),
    "QWEN_BASE_URL": os.getenv("QWEN_BASE_URL", "https://coding.dashscope.aliyuncs.com/v1"),
    "DEEPSEEK_API_KEY": os.getenv("DEEPSEEK_API_KEY", ""),
    "DEEPSEEK_BASE_URL": os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
    "GPT_API_KEY": os.getenv("GPT_API_KEY", ""),
    "GPT_BASE_URL": os.getenv("GPT_BASE_URL", "https://api.aiearth.dev/v1"),

    "API_PROVIDER": "gemini",
    "MODEL_NAME": "gemini-3-flash-preview",

    "BASE_PATH": BASE_DATA_PATH or DATA_DIR,
    "PATHS": {
        "PARSED_DATASET": _resolve(
            "test_dataset/test_dataset.csv",
            "BigQuery_since20251230/new_data_0117/cleaned_data/test_dataset_2024Q4_1000_0209.csv"),
        "TOKEN_KB": _resolve(
            "token_metadata/token_metadata.csv",
            "verified_permit/labeled_address/features/Top_1000_Tokens_Decimals.csv"),
        "ADDRESS_LABELS": _resolve(
            "labeled_address/labeled_addresses.csv",
            "BigQuery_since20251230/new_data_0117/cleaned_data/labeled_address/all_labeled_address_883071.csv"),
        "HISTORY_DIR": _resolve(
            "spender_history",
            "verified_permit/labeled_address/1_order_0609"),
        "SPENDER_FEATURES": _resolve(
            "feature_profiles/spender_features.csv",
            "BigQuery_since20251230/analysis/spender_analysis_result_3months_0209.csv"),
        "SUBMITTER_FEATURES": _resolve(
            "feature_profiles/submitter_features.csv",
            "BigQuery_since20251230/analysis/original_submitter_analysis_result_3months_0211.csv"),
        "TRANSFER_FEATURES": _resolve(
            "feature_profiles/transfer_features.csv",
            "BigQuery_since20251230/analysis/transfer_analysis_result_3months_0209.csv"),
        "COMBINED_FEATURES": _resolve(
            "feature_profiles/interaction_features.csv",
            "BigQuery_since20251230/analysis/interaction_features_result_3months_0209.csv"),
        "PIPELINE_OUTPUT_DIR": PIPELINE_OUTPUT_DIR,
        "RAW_TRACES_JSONL": os.path.join(PIPELINE_OUTPUT_DIR, "raw_traces", "permit_traces.jsonl"),
        "PIPELINE_CLEANED_DIR": os.path.join(PIPELINE_OUTPUT_DIR, "raw_traces", "cleaned"),
        "PIPELINE_SPENDER_HISTORY": os.path.join(PIPELINE_OUTPUT_DIR, "spender_history"),
        "PIPELINE_TOKEN_METADATA": os.path.join(PIPELINE_OUTPUT_DIR, "token_metadata"),
        "OUTPUT_REPORT": os.path.join(RESULT_DIR, "reports", "PermitGuard_Audit_Report.json"),
        "FP_OUTPUT": os.path.join(RESULT_DIR, "error_analysis", "FP_Cases.json"),
        "FN_OUTPUT": os.path.join(RESULT_DIR, "error_analysis", "FN_Cases.json"),
        "FIGURE_DIR": os.path.join(RESULT_DIR, "figures"),
    },

    "THRESHOLDS": {
        "HIGH_VALUE_USD": 10_000,
        "LONG_DEADLINE_SECONDS": 365,
    },
}
