import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
PIPELINE_OUTPUT_DIR = os.path.join(DATA_DIR, "pipeline_output")
RESULT_DIR = os.path.join(PROJECT_ROOT, "result")

BASE_DATA_PATH = os.getenv("BASE_DATA_PATH", "")

def _data_path(relative: str) -> str:
    """Resolve a path relative to the data directory (or BASE_DATA_PATH if set)."""
    base = BASE_DATA_PATH or DATA_DIR
    return os.path.join(base, relative)

CONFIG = {
    "GEMINI_API_KEY": os.getenv("GEMINI_API_KEY", ""),
    "QWEN_API_KEY": os.getenv("QWEN_API_KEY", ""),
    "QWEN_BASE_URL": os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
    "DEEPSEEK_API_KEY": os.getenv("DEEPSEEK_API_KEY", ""),
    "DEEPSEEK_BASE_URL": os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
    "GPT_API_KEY": os.getenv("GPT_API_KEY", ""),
    "GPT_BASE_URL": os.getenv("GPT_BASE_URL", "https://api.openai.com/v1"),

    "API_PROVIDER": "gemini",
    "MODEL_NAME": "gemini-3-flash-preview",

    "BASE_PATH": BASE_DATA_PATH or DATA_DIR,
    "PATHS": {
        "PARSED_DATASET": _data_path("test_dataset/test_dataset.csv"),
        "TOKEN_KB": _data_path("token_metadata/token_metadata.csv"),
        "ADDRESS_LABELS": _data_path("labeled_address/labeled_addresses.csv"),
        "HISTORY_DIR": _data_path("spender_history"),
        "SPENDER_FEATURES": _data_path("feature_profiles/spender_features.csv"),
        "SUBMITTER_FEATURES": _data_path("feature_profiles/submitter_features.csv"),
        "TRANSFER_FEATURES": _data_path("feature_profiles/transfer_features.csv"),
        "COMBINED_FEATURES": _data_path("feature_profiles/interaction_features.csv"),
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
