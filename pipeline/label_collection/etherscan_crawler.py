"""
Stage 3: Etherscan nametag crawling and address labeling.
Crawls Etherscan public pages to retrieve nametags and contract types,
then assigns labels (benign=2, malicious=1, no_tag=0).
"""

import os
import sys
import time
import csv
import random
import warnings

import requests
import pandas as pd
from bs4 import BeautifulSoup

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from src.utils.config import CONFIG

BASE_PATH = CONFIG["BASE_PATH"]
BQ_PATH = CONFIG["PATHS"]["PIPELINE_OUTPUT_DIR"]


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

MALICIOUS_KEYWORDS = [
    'phish', 'hack', 'exploit', 'scam', 'drainer', 'fake_phishing',
    'ofac', 'heist', 'theft', 'ponzi',
]


def get_etherscan_nametag(address: str) -> dict:
    """Fetch nametag for a single address from Etherscan public page."""
    url = f"https://etherscan.io/address/{address}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return {"address": address, "nametag": None, "label": None}

        soup = BeautifulSoup(resp.text, 'html.parser')
        nametag_elem = soup.find('span', class_='hash-tag')
        if not nametag_elem:
            return {"address": address, "nametag": None, "label": 0}

        nametag = nametag_elem.text.strip()

        if any(kw in nametag.lower() for kw in MALICIOUS_KEYWORDS):
            label = 1
        elif nametag:
            label = 2
        else:
            label = 0

        return {"address": address, "nametag": nametag, "label": label}

    except Exception as e:
        print(f"  Error fetching {address}: {e}")
        return {"address": address, "nametag": None, "label": None}


def batch_crawl(input_csv: str, output_csv: str, batch_size: int = 50, delay: float = 2.0):
    """
    Batch-crawl nametags from a list of addresses.
    Supports checkpointing: skips already-labeled addresses.
    """
    df = pd.read_csv(input_csv)
    df['address'] = df['address'].astype(str).str.lower().str.strip()

    already_done = set()
    if os.path.exists(output_csv):
        df_done = pd.read_csv(output_csv)
        already_done = set(df_done['address'].str.lower())
        print(f"Checkpoint: {len(already_done)} addresses already processed.")

    addresses = [a for a in df['address'].unique() if a not in already_done]
    print(f"Remaining: {len(addresses)} addresses to crawl.")

    results = []
    for i, addr in enumerate(addresses):
        result = get_etherscan_nametag(addr)
        results.append(result)

        if (i + 1) % batch_size == 0:
            _save_batch(results, output_csv)
            results.clear()
            print(f"  Checkpoint saved: {i + 1}/{len(addresses)}")

        time.sleep(delay + random.uniform(0, 1))

    if results:
        _save_batch(results, output_csv)

    print(f"Crawling complete. Output: {output_csv}")


def _save_batch(results, output_csv):
    df = pd.DataFrame(results)
    header = not os.path.exists(output_csv)
    df.to_csv(output_csv, mode='a', index=False, header=header)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Etherscan nametag crawler")
    parser.add_argument("--input", required=True, help="CSV with 'address' column")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--batch_size", type=int, default=50)
    parser.add_argument("--delay", type=float, default=2.0)
    args = parser.parse_args()

    batch_crawl(args.input, args.output, args.batch_size, args.delay)
