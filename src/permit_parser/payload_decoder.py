import pandas as pd
from typing import Dict, Any

from src.utils.config import CONFIG
from src.permit_parser.context_retriever import KnowledgeBase
from src.permit_parser.trace_fetcher import ForensicsAnalyzer


class DeepFactEnricher:
    """Transforms raw transaction rows into enriched fact sheets with statistical significance."""

    def __init__(self):
        self.kb = KnowledgeBase()
        self.forensics = ForensicsAnalyzer(CONFIG["PATHS"]["HISTORY_DIR"])

    def enrich(self, raw_row: pd.Series) -> Dict[str, Any]:
        tx_hash = raw_row.get('tx_hash', '')
        ts = int(raw_row.get('timestamp', 0))
        block_number = raw_row.get('block_number', '')
        submitter = str(raw_row.get('original_submitter', '')).lower()
        relayer = str(raw_row.get('relayer', '')).lower()
        relayer_label = int(raw_row.get('relayer_label', 0))
        relayer_nametag = str(raw_row.get('relayer_nametag', 'Unknown')).lower()
        owner = str(raw_row.get('permit_owner', '')).lower()
        spender = str(raw_row.get('permit_spender', '')).lower()
        token = str(raw_row.get('token_address', '')).lower()
        value_raw = raw_row.get('permit_value', 0)
        deadline = int(raw_row.get('permit_deadline', 0))
        transfer_to_label = int(raw_row.get('transfer_to_label', 0))
        transfer_to_nametag = str(raw_row.get('transfer_to_nametag', 0))
        permit_trace = raw_row.get('permit_trace', '')
        transfer_trace = raw_row.get('transfer_trace', '')

        # --- Submitter Profile ---
        sb_profile = self.kb.get_submitter_profile(submitter)
        is_self_submitted = (submitter == owner)

        submitter_context = {
            "address": submitter,
            "label_submitter": sb_profile.get("label_submitter", 0),
            "nametag_submitter": sb_profile.get("nametag", 0),
            "relationship_to_owner": "Self (Owner)" if is_self_submitted else "Third-Party (Relayer/Worker)",
            "total_txs": sb_profile.get("total_txs", 0),
            "unique_owners": sb_profile.get("unique_victims", 0),
            "unique_spenders": sb_profile.get("unique_spenders", 0),
            "ratio_mediated": round(sb_profile.get("ratio_mediated", 0), 4),
            "sb_high_value_ratio": round(sb_profile.get("ratio_high_value", 0), 4),
            "feat_lp_token": sb_profile.get("feat_lp_token", 0),
            "unique_lp_tokens": sb_profile.get("unique_lp_tokens", 0),
        }

        # --- Relayer Profile ---
        relayer_context = {
            "address": relayer,
            "label_relayer": relayer_label,
            "nametag_relayer": relayer_nametag,
        }

        # --- Spender Profile ---
        sp_profile = self.kb.get_spender_profile(spender)
        sp_type = sp_profile.get("type", "Unknown")
        raw_tx_count = sp_profile.get("total_tx_count", 0)
        is_ghost = bool(sp_profile.get("is_ghost", False))
        sp_note = "Standard Contract"

        if raw_tx_count == -1:
            is_ghost = False
            sp_type = "High-Volume Protocol (Legendary)"
            sp_note = "Verified Infrastructure (>1M txs)"
        elif spender == "0x0000000000000000000000000000000000000001":
            is_ghost = False
            sp_type = "System Precompile"
            sp_note = "Ethereum System Address (Safe)"

        spender_context = {
            "address": spender,
            "label_spender": sp_profile.get("label_spender", 0),
            "nametag_spender": sp_profile.get("nametag", 0),
            "address_type": sp_type,
            "special_note": sp_note,
            "total_tx_count": "1,000,000+" if raw_tx_count == -1 else raw_tx_count,
            "is_ghost": is_ghost,
            "flow_ratio": round(sp_profile.get("token_flow_ratio", 0), 4),
            "payer_concentration": round(sp_profile.get("top1_gas_payer_ratio", 0), 4),
        }

        # --- Intent Features ---
        token_info = self.kb.get_token_info(token)
        token_decimals = token_info['decimals']
        token_price = token_info['price_usd']

        if token_decimals < 0 or token_decimals > 256:
            token_decimals = 18
        try:
            human_amount = value_raw / (10 ** token_decimals)
            permit_usd_value = human_amount * token_price
        except Exception:
            permit_usd_value = 0.0

        is_infinite_amount = (value_raw > 1e50)

        current_time = ts
        time_diff = (deadline - current_time) / 86400
        if time_diff > CONFIG["THRESHOLDS"]["LONG_DEADLINE_SECONDS"]:
            validity_str = f"{time_diff:.2f} Days"
            is_infinite_time = True
        elif time_diff < 0:
            validity_str = "Expired"
            is_infinite_time = False
        else:
            validity_str = f"{time_diff:.2f} Days"
            is_infinite_time = False

        # --- Execution Features ---
        cf_profile = self.kb.get_combined_profile(tx_hash, permit_trace, transfer_trace)
        execution_status = cf_profile.get("execution_status")

        transfer_to_addr = "None"
        transfer_raw_amount = 0.0
        drain_tx_hash = "None"

        if execution_status == "Atomic":
            transfer_to_addr = str(raw_row.get('transfer_to', '')).lower()
            transfer_raw_amount = raw_row.get('transfer_amount_hex', 0)
        else:
            drain_check = self.forensics.check_future_drain(spender, owner, ts)
            if drain_check.get('confirmed'):
                drain_tx_hash = drain_check.get('tx_hash', 'Unknown')
                transfer_to_addr = str(drain_check.get('to_address', '')).lower()
                transfer_raw_amount = float(drain_check.get('amount', 0))

        if transfer_raw_amount > 0:
            transfer_human_amount = transfer_raw_amount / (10 ** token_decimals)
            transfer_usd_value = transfer_human_amount * token_price
        else:
            transfer_usd_value = 0.0

        permit_transfer_ratio = cf_profile.get("feat_7_utilization")

        combined_context = {
            "unverified_infinite": cf_profile.get("feat_unverified_infinite"),
            "time_reputation_div": cf_profile.get("feat_time_reputation_div"),
            "relayed_theft": cf_profile.get("feat_relayed_theft"),
            "signature_harvesting": cf_profile.get("feat_signature_harvesting"),
        }

        # --- Transfer Type Classification ---
        execution_context = {
            "execution_status": execution_status,
            "drain_tx": drain_tx_hash,
            "transfer_to": transfer_to_addr,
            "label_transfer_to": transfer_to_label,
            "nametag_transfer_to": transfer_to_nametag,
            "transfer_raw_value": transfer_raw_amount,
            "transfer_usd_value": f"${transfer_usd_value:,.2f}",
            "permit_transfer_ratio": f"{permit_transfer_ratio * 100: .2f}%" if permit_transfer_ratio else "0.00%",
            "transfer_type": "Unknown",
        }

        if execution_status in ("Atomic", "Used_Delayed"):
            if transfer_to_addr == spender:
                execution_context["transfer_type"] = "Self-Loop (To Spender)"
            elif transfer_to_addr == submitter:
                execution_context["transfer_type"] = "Kickback/Sweep (To Submitter)"
            elif transfer_to_addr == relayer:
                execution_context["transfer_type"] = "Solver-Settlement (To Relayer)"
            elif transfer_to_addr == owner:
                execution_context["transfer_type"] = "Self-Rescue (To Owner)"
            elif transfer_to_addr == token:
                execution_context["transfer_type"] = "Reflection (To Token)"
            elif transfer_to_addr and transfer_to_addr != "none":
                execution_context["transfer_type"] = f"Third-Party Transfer (To {transfer_to_addr}...)"
        else:
            execution_context["transfer_type"] = "Delayed and No Execution"

        return {
            "metadata": {"tx_hash": tx_hash, "block_number": block_number, "timestamp": ts},
            "permit_intent": {
                "token_address": token,
                "token_symbol": token_info.get("symbol", "UNK"),
                "permit_raw_value": f"{value_raw:.0f}",
                "permit_usd_value": f"${permit_usd_value:,.2f}",
                "validity_period": validity_str,
                "risk_flags": {"is_infinite_amount": is_infinite_amount, "is_infinite_time": is_infinite_time},
            },
            "entities": {
                "submitter": submitter_context,
                "relayer": relayer_context,
                "spender": spender_context,
                "owner_address": owner,
            },
            "combined features": combined_context,
            "execution_forensics": execution_context,
        }
