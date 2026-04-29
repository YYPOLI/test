import os
import pandas as pd
from typing import Dict, Any

from src.utils.config import CONFIG


class ForensicsAnalyzer:
    """Queries local historical transaction data to determine ghost status and delayed drain behavior."""

    def __init__(self, history_dir: str = None):
        self.history_dir = history_dir or CONFIG["PATHS"]["HISTORY_DIR"]
        self.transfer_from_selector = "0x23b872dd"
        self.cache = {}

    def _load_history(self, address: str) -> pd.DataFrame:
        if address in self.cache:
            return self.cache[address]
        try:
            path = os.path.join(self.history_dir, f"{address}/{address}_external.csv")
            if os.path.exists(path):
                df = pd.read_csv(path, low_memory=False)
                df['timeStamp'] = pd.to_numeric(df['timeStamp'], errors='coerce')
                self.cache[address] = df
                return df
        except Exception:
            pass
        self.cache[address] = pd.DataFrame()
        return self.cache[address]

    def check_ghost_status(self, address: str) -> Dict[str, Any]:
        """Check whether an address is a ghost contract (no external transaction history)."""
        df = self._load_history(address)
        tx_count = len(df)
        is_ghost = (tx_count == 0)
        return {
            "is_ghost": is_ghost,
            "tx_count": tx_count,
            "status_desc": "Ghost Contract (High Risk)" if is_ghost else f"Active Entity ({tx_count} txs)",
        }

    def check_future_drain(self, spender: str, victim: str, permit_time: int) -> Dict[str, Any]:
        """
        Check whether a delayed transferFrom (drain) occurred after the permit was signed.
        Parses ERC-20 transferFrom(from, to, amount) input data to extract transfer_to and amount.
        """
        df = self._load_history(spender)
        if df.empty:
            return {"confirmed": False, "type": "No History"}

        future_txs = df[df['timeStamp'].astype(int) > permit_time]
        target_method_id = "0x23b872dd"
        victim_clean = victim.lower().replace("0x", "")

        for _, tx in future_txs.iterrows():
            if str(tx.get('from', '')).lower() != spender.lower():
                continue

            input_data = str(tx.get('input', ''))
            if not input_data.startswith(target_method_id) or len(input_data) < 202:
                continue

            try:
                param_from = input_data[10:74]
                extracted_victim = param_from[24:].lower()

                if extracted_victim == victim_clean:
                    param_to = input_data[74:138]
                    to_address = "0x" + param_to[24:].lower()
                    param_amount = input_data[138:202]
                    amount_val = int(param_amount, 16)

                    return {
                        "confirmed": True,
                        "type": "Delayed Drain Detected",
                        "tx_hash": tx.get('hash', tx.get('tx_hash', 'Unknown')),
                        "to_address": to_address,
                        "amount": float(amount_val),
                    }
            except Exception:
                continue

        return {"confirmed": False, "type": "Atomic or No Action"}
