"""
Stage 4 (supplement): Token economic value mapping (§4.1).

Fetches Top 1000 tokens by market cap from CoinGecko, maps their Ethereum
contract addresses, then queries on-chain decimals via an Ethereum RPC node.
Outputs data/token_metadata/Top_1000_Tokens_Decimals.csv — consumed by the
ContextRetriever (src/permit_parser/context_retriever.py).

Prerequisites:
  - CoinGecko free API (no key needed, but rate-limited ~10-30 req/min).
  - An Ethereum RPC endpoint (set ETH_RPC_URL in .env; defaults to public).

Usage:
    python pipeline/feature_engineering/token_metadata_crawler.py
    python pipeline/feature_engineering/token_metadata_crawler.py --skip-decimals
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

OUTPUT_DIR = CONFIG["PATHS"]["PIPELINE_TOKEN_METADATA"]
OUTPUT_MARKETS = os.path.join(OUTPUT_DIR, "token_markets.csv")
OUTPUT_FINAL = os.path.join(OUTPUT_DIR, "token_metadata.csv")

ETH_RPC_URL = os.getenv("ETH_RPC_URL", "https://eth.llamarpc.com")
DECIMALS_SELECTOR = "0x313ce567"


def _api_get_with_retry(url, params=None, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
            wait = (2 ** attempt) + random.uniform(0, 1)
            print(f"  Retry {attempt+1}/{max_retries} after {wait:.1f}s — {e}")
            time.sleep(wait)
    return None


def fetch_market_data(num_tokens=1000):
    """Get Top N tokens by market cap from CoinGecko."""
    per_page = 250
    pages = (num_tokens + per_page - 1) // per_page
    market_data = []

    print(f"Fetching Top {num_tokens} token market data ({pages} pages)...")
    for page in range(1, pages + 1):
        params = {
            "vs_currency": "usd", "order": "market_cap_desc",
            "per_page": per_page, "page": page, "sparkline": "false",
        }
        data = _api_get_with_retry("https://api.coingecko.com/api/v3/coins/markets", params)
        if data:
            market_data.extend(data)
            print(f"  Page {page}/{pages}: {len(data)} tokens.")
        else:
            print(f"  Page {page}/{pages}: FAILED, skipping.")
        if page < pages:
            time.sleep(1.5)

    df = pd.DataFrame(market_data)
    return df[["id", "symbol", "name", "current_price", "market_cap"]]


def fetch_contract_addresses():
    """Get Ethereum contract addresses for all CoinGecko tokens."""
    print(f"\nFetching Ethereum contract address list...")
    url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
    data = _api_get_with_retry(url)
    if not data:
        print("  FAILED to fetch address list.")
        return pd.DataFrame(columns=["id", "contract_address"])

    rows = []
    for coin in data:
        eth_addr = coin.get("platforms", {}).get("ethereum")
        if eth_addr:
            rows.append({"id": coin["id"], "contract_address": eth_addr})
    print(f"  Found {len(rows)} tokens with Ethereum addresses.")
    return pd.DataFrame(rows)


def query_decimals(df):
    """Query on-chain decimals for each contract address via Ethereum RPC."""
    print(f"\nQuerying on-chain decimals for {len(df)} tokens...")
    decimals_list = []

    for i, row in df.iterrows():
        addr = row.get("contract_address", "")
        if not isinstance(addr, str) or not addr.startswith("0x"):
            decimals_list.append(None)
            continue

        payload = {
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": addr, "data": DECIMALS_SELECTOR}, "latest"],
            "id": 1,
        }
        try:
            resp = requests.post(ETH_RPC_URL, json=payload, timeout=5)
            result = resp.json().get("result")
            decimals_list.append(int(result, 16) if result and result != "0x" else None)
        except Exception:
            decimals_list.append(None)

        if (i + 1) % 50 == 0:
            sys.stdout.write(f"\r  Progress: {i+1}/{len(df)}")
            sys.stdout.flush()
        time.sleep(0.1)

    print()
    df["decimals"] = decimals_list
    return df


def main(skip_decimals=False):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df_markets = fetch_market_data()
    df_addresses = fetch_contract_addresses()

    df_merged = pd.merge(df_markets, df_addresses, on="id", how="left")
    df_merged["contract_address"] = df_merged["contract_address"].fillna("N/A")

    df_merged.to_csv(OUTPUT_MARKETS, index=False)
    print(f"\n  Market data saved: {OUTPUT_MARKETS}")

    if skip_decimals:
        print("  Skipping decimals query (--skip-decimals).")
        return

    df_final = query_decimals(df_merged)
    df_final.to_csv(OUTPUT_FINAL, index=False)
    print(f"  Final output saved: {OUTPUT_FINAL}")
    print(f"\n[Done] {len(df_final)} tokens with decimals -> {OUTPUT_FINAL}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 4: Token metadata crawler (CoinGecko)")
    parser.add_argument("--skip-decimals", action="store_true",
                        help="Only fetch market data, skip on-chain decimals query")
    args = parser.parse_args()
    main(args.skip_decimals)
