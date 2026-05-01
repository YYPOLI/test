import pandas as pd
from typing import Dict

from src.utils.config import CONFIG


class ContextRetriever:
    """
    Context Retriever (§5.2): responsible for supplementing the external knowledge base K.
    Bridges on-chain transactions with external environmental knowledge including
    token metadata, fiat prices, and dynamically updated address label library.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.tokens = {}
            cls._instance.phishers = set()
            cls._instance.benign = set()
            cls._instance.no_label = set()
            cls._instance.label_map = {}
            cls._instance.name_map = {}
            cls._instance.spender_features = {}
            cls._instance.submitter_features = {}
            cls._instance.combined_features = {}
            cls._instance._load_data()
        return cls._instance

    def _load_data(self):
        print("[KB] Loading knowledge bases...")
        self._load_tokens()
        self._load_labels()
        self._load_spender_features()
        self._load_submitter_features()
        self._load_combined_features()

    def _load_tokens(self):
        try:
            df = pd.read_csv(CONFIG["PATHS"]["TOKEN_KB"])
            df["contract_address"] = df["contract_address"].astype(str).str.lower().str.strip()
            for _, row in df.iterrows():
                self.tokens[row["contract_address"]] = {
                    "symbol": row.get("symbol", "UNK"),
                    "decimals": int(row.get("decimals", 18)) if pd.notna(row.get("decimals")) else 18,
                    "price_usd": float(row.get("current_price", 0)) if pd.notna(row.get("current_price")) else 0.0,
                }
            print(f"   - Token KB: {len(self.tokens)} loaded.")
        except Exception as e:
            print(f"   [Error] Token KB load failed: {e}")

    def _load_labels(self):
        try:
            df = pd.read_csv(CONFIG["PATHS"]["ADDRESS_LABELS"], encoding_errors='replace')
            df['nametag'] = df['nametag'].fillna('Unknown')
            self.phishers = set(df[df["label"] == 1]["address"].str.lower())
            self.benign = set(df[df["label"] == 2]["address"].str.lower())
            self.no_label = set(df[df["label"] == 0]["address"].str.lower())
            self.label_map = df.set_index('address')['label'].to_dict()
            self.name_map = df.set_index('address')['nametag'].to_dict()
            print(f"   - Labels: {len(self.phishers)} Phishers, {len(self.benign)} Benign, {len(self.no_label)} No Label.")
        except Exception as e:
            print(f"   [Error] Label KB load failed: {e}")

    def _load_spender_features(self):
        try:
            df = pd.read_csv(CONFIG["PATHS"]["SPENDER_FEATURES"])
            df["address"] = df["address"].str.lower().str.strip()
            self.spender_features = df.set_index("address").to_dict(orient="index")
            print(f"   - Spender Features: {len(self.spender_features)} profiles loaded")
        except Exception as e:
            print(f"   [Error] Spender Features load failed: {e}")

    def _load_submitter_features(self):
        try:
            df = pd.read_csv(CONFIG["PATHS"]["SUBMITTER_FEATURES"])
            df["original_submitter"] = df["original_submitter"].str.lower().str.strip()
            df['nametag'] = df['original_submitter'].map(self.name_map)
            self.submitter_features = df.set_index("original_submitter").to_dict(orient="index")
            print(f"   - Submitter Features: {len(self.submitter_features)} profiles loaded")
        except Exception as e:
            print(f"   [Error] Submitter Features load failed: {e}")

    def _load_combined_features(self):
        try:
            df = pd.read_csv(CONFIG["PATHS"]["COMBINED_FEATURES"])
            self.combined_features = df.set_index(["tx_hash", "permit_trace", "transfer_trace"]).to_dict(orient="index")
            print(f"   - Combined Features: {len(self.combined_features)} profiles loaded")
        except Exception as e:
            print(f"   [Error] Combined Features load failed: {e}")

    def get_token_info(self, address: str) -> Dict:
        return self.tokens.get(address.lower(), {"symbol": "Unknown", "decimals": 18, "price_usd": 0})

    def get_label_status(self, address: str) -> str:
        addr = address.lower()
        if addr in self.phishers:
            return "Phishing"
        if addr in self.benign:
            return "Benign/Whitelist"
        if addr in self.no_label:
            return "No Label"
        return "Unknown"

    def get_spender_profile(self, addr: str) -> Dict:
        return self.spender_features.get(addr.lower(), {})

    def get_submitter_profile(self, addr: str) -> Dict:
        return self.submitter_features.get(addr.lower(), {})

    def get_combined_profile(self, txhash: str, permit_trace, transfer_trace) -> Dict:
        return self.combined_features.get((txhash.lower(), permit_trace, transfer_trace), {})
