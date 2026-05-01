"""
Stage 2 (supplement): Spender historical transaction crawler (§4.1).

For each spender address extracted from the cleaned permit dataset, retrieves
three types of historical transactions via the Etherscan API:
  - External (normal) transactions  → {address}_external.csv
  - Internal transactions           → {address}_internal.csv
  - ERC-20 token transfers          → {address}_tokentx.csv

These files are saved under data/spender_history/{address}/ and consumed by
the TraceFetcher (src/permit_parser/trace_fetcher.py) for delayed-drain
detection and ghost-spender identification.

Prerequisites:
  - Set ETHERSCAN_API_KEY in .env (free tier: 5 calls/sec).
  - The address list is extracted from the Stage 2 cleaned CSV.

Usage:
    python pipeline/data_processing/spender_history_crawler.py
    python pipeline/data_processing/spender_history_crawler.py --input data/raw_traces/cleaned/cleaned_all.csv
    python pipeline/data_processing/spender_history_crawler.py --large   # for addresses with >10k txs
"""

import os
import sys
import time
import random
import argparse

import pandas as pd
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from src.utils.config import CONFIG

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
BASE_URL = "https://api.etherscan.io/v2/api"
END_BLOCK = 99999999


# ---------------------------------------------------------------------------
# DataFrame parsers (vectorized)
# ---------------------------------------------------------------------------

def _parse_external(results):
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).rename(columns={"hash": "txHash"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0) / 1e18
    cols = ["txHash", "blockNumber", "timeStamp", "from", "to", "value",
            "gas", "gasPrice", "gasUsed", "isError", "input",
            "contractAddress", "methodId", "functionName"]
    return df.reindex(columns=cols)


def _parse_internal(results):
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).rename(columns={"hash": "txHash"})
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0) / 1e18
    cols = ["txHash", "blockNumber", "timeStamp", "from", "to", "value",
            "contractAddress", "input", "type", "gas", "gasUsed", "isError"]
    return df.reindex(columns=cols)


def _parse_tokentx(results):
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).rename(columns={"hash": "txHash"})
    df["raw_value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)
    df["tokenDecimal"] = pd.to_numeric(df["tokenDecimal"], errors="coerce").fillna(18)
    df["value"] = df["raw_value"] / (10 ** df["tokenDecimal"])
    cols = ["txHash", "blockNumber", "timeStamp", "nonce", "from",
            "contractAddress", "to", "value", "tokenName", "tokenSymbol",
            "tokenDecimal", "transactionIndex", "gas", "gasPrice", "gasUsed",
            "cumulativeGasUsed", "input", "methodId", "functionName"]
    return df.reindex(columns=cols)


# ---------------------------------------------------------------------------
# API fetch helpers
# ---------------------------------------------------------------------------

def _api_get(params, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f"  Retry {attempt+1}/{max_retries} after {wait:.1f}s — {e}")
            time.sleep(wait)
    return None


def _fetch_standard(action, address, parser):
    params = {
        "chainid": 1, "module": "account", "action": action,
        "address": address, "startblock": 0, "endblock": END_BLOCK,
        "sort": "asc", "apikey": ETHERSCAN_API_KEY,
    }
    data = _api_get(params)
    if data and data.get("status") == "1":
        return parser(data["result"])
    return parser([])


def _fetch_large(action, address, parser):
    """Dynamic block-stepping for addresses with >10k transactions."""
    all_results = []
    start_block = 0

    for _ in range(500):
        params = {
            "chainid": 1, "module": "account", "action": action,
            "address": address, "startblock": start_block, "endblock": END_BLOCK,
            "sort": "asc", "apikey": ETHERSCAN_API_KEY,
        }
        data = _api_get(params)
        if not data or data.get("status") != "1" or not data.get("result"):
            break

        batch = data["result"]
        all_results.extend(batch)

        if len(batch) >= 10000:
            last_block = int(batch[-1]["blockNumber"])
            start_block = last_block if last_block != start_block else last_block + 1
        else:
            break
        time.sleep(0.25)

    df = parser(all_results)
    if not df.empty and "txHash" in df.columns:
        if action != "tokentx":
            df.drop_duplicates(subset=["txHash"], inplace=True)
        else:
            df.drop_duplicates(inplace=True)
    return df


# ---------------------------------------------------------------------------
# Single-address loader
# ---------------------------------------------------------------------------

def fetch_address(address, output_dir, use_large=False):
    """Fetch and save all three tx types for one address."""
    addr_dir = os.path.join(output_dir, address)
    f_ex = os.path.join(addr_dir, f"{address}_external.csv")
    f_in = os.path.join(addr_dir, f"{address}_internal.csv")
    f_tk = os.path.join(addr_dir, f"{address}_tokentx.csv")

    if os.path.exists(f_ex) and os.path.exists(f_in) and os.path.exists(f_tk):
        return "cached"

    os.makedirs(addr_dir, exist_ok=True)
    fetch = _fetch_large if use_large else _fetch_standard

    need_ex = not os.path.exists(f_ex)
    need_in = not os.path.exists(f_in)
    need_tk = not os.path.exists(f_tk)

    if need_ex:
        df_ex = fetch("txlist", address, _parse_external)
        if not df_ex.empty:
            df_ex = df_ex[df_ex["isError"] != "1"].sort_values("timeStamp").reset_index(drop=True)
        df_ex.to_csv(f_ex, index=False)

    if need_in:
        df_in = fetch("txlistinternal", address, _parse_internal)
        if not df_in.empty:
            df_in = df_in[df_in["isError"] != "1"].sort_values("timeStamp").reset_index(drop=True)
        df_in.to_csv(f_in, index=False)

    if need_tk:
        df_tk = fetch("tokentx", address, _parse_tokentx)
        if not df_tk.empty:
            df_tk = df_tk.sort_values("timeStamp").reset_index(drop=True)
        df_tk.to_csv(f_tk, index=False)

    return "fetched"


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

def run(input_csv=None, output_dir=None, use_large=False):
    if not ETHERSCAN_API_KEY:
        print("[Error] Set ETHERSCAN_API_KEY in .env first.")
        sys.exit(1)

    if output_dir is None:
        output_dir = CONFIG["PATHS"]["PIPELINE_SPENDER_HISTORY"]
    if input_csv is None:
        input_csv = CONFIG["PATHS"].get("PIPELINE_CLEANED_DIR", "")
        input_csv = os.path.join(input_csv, "cleaned_all.csv") if os.path.isdir(input_csv) else input_csv

    print(f"[Spender Crawler] Input : {input_csv}")
    print(f"[Spender Crawler] Output: {output_dir}")
    print(f"[Spender Crawler] Mode  : {'large-address' if use_large else 'standard'}")

    df = pd.read_csv(input_csv)
    spenders = df["permit_spender"].dropna().unique()
    print(f"[Spender Crawler] {len(spenders)} unique spender addresses to crawl.\n")

    for i, addr in enumerate(spenders):
        addr = str(addr).lower().strip()
        status = fetch_address(addr, output_dir, use_large)
        print(f"  [{i+1}/{len(spenders)}] {addr} — {status}")
        if status == "fetched":
            time.sleep(0.25)

    print(f"\n[Done] Spender history saved to {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 2: Spender history crawler")
    parser.add_argument("--input", default=None, help="Cleaned CSV with permit_spender column")
    parser.add_argument("--output-dir", default=None, help="Output directory for spender history")
    parser.add_argument("--large", action="store_true", help="Use large-address mode (block stepping)")
    args = parser.parse_args()
    run(args.input, args.output_dir, args.large)
