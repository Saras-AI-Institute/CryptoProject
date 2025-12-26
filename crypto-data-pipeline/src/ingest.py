import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
API_KEY_ENV_VAR = "COINGECKO_API_KEY"

PARAMS = {
    "vs_currency": "usd",
    "ids": "bitcoin,ethereum,solana",
    "order": "market_cap_desc",
    "per_page": 3,
    "page": 1
}

OUTPUT_DIR = Path("landing_zone")
OUTPUT_FILE = OUTPUT_DIR / "crypto_prices_sample.json"


def _build_headers() -> dict:
    """Return request headers, injecting API key from environment when provided."""
    api_key = os.getenv(API_KEY_ENV_VAR)
    if not api_key:
        return {}
    # CoinGecko Pro uses the x-cg-pro-api-key header for authentication
    return {"x-cg-pro-api-key": api_key}


def fetch_crypto_data(retries=3, backoff=5):
    for attempt in range(retries):
        try:
            response = requests.get(
                API_URL,
                params=PARAMS,
                headers=_build_headers(),
                timeout=10,
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                print("⚠️ Rate limit hit. Retrying...")
                time.sleep(backoff)
                continue

            response.raise_for_status()

        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(backoff)

    raise Exception("Failed after multiple retries")


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)

    data = fetch_crypto_data()

    payload = {
        "ingested_at": datetime.utcnow().isoformat(),
        "source": "CoinGecko",
        "records": data
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"✅ Data saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
