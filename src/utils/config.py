import os
from dotenv import load_dotenv

load_dotenv()

BASE_DATA_PATH = os.getenv("BASE_DATA_PATH", "E:/Dataset/Phishing/")

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

    "BASE_PATH": BASE_DATA_PATH,
    "PATHS": {
        "PARSED_DATASET": os.path.join(BASE_DATA_PATH, "BigQuery_since20251230/new_data_0117/cleaned_data/test_dataset_2024Q4_1000_0209.csv"),
        "TOKEN_KB": os.path.join(BASE_DATA_PATH, "verified_permit/labeled_address/features/Top_1000_Tokens_Decimals.csv"),
        "ADDRESS_LABELS": os.path.join(BASE_DATA_PATH, "BigQuery_since20251230/new_data_0117/cleaned_data/labeled_address/all_labeled_address_883071.csv"),
        "HISTORY_DIR": os.path.join(BASE_DATA_PATH, "verified_permit/labeled_address/1_order_0609/"),
        "SPENDER_FEATURES": os.path.join(BASE_DATA_PATH, "BigQuery_since20251230/analysis/spender_analysis_result_3months_0209.csv"),
        "SUBMITTER_FEATURES": os.path.join(BASE_DATA_PATH, "BigQuery_since20251230/analysis/original_submitter_analysis_result_3months_0211.csv"),
        "TRANSFER_FEATURES": os.path.join(BASE_DATA_PATH, "BigQuery_since20251230/analysis/transfer_analysis_result_3months_0209.csv"),
        "COMBINED_FEATURES": os.path.join(BASE_DATA_PATH, "BigQuery_since20251230/analysis/interaction_features_result_3months_0209.csv"),
        "OUTPUT_REPORT": "PermitGuard_Audit_Report.json",
        "FP_OUTPUT": "FP_Cases.json",
        "FN_OUTPUT": "FN_Cases.json",
    },

    "THRESHOLDS": {
        "HIGH_VALUE_USD": 10_000,
        "LONG_DEADLINE_SECONDS": 365,
    },
}
