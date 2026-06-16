import os
import time
import traceback

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "nifty-beast-realm"
PRICE_TABLE = os.getenv("PRICE_TABLE", "magic.scryfall-prices-v2")
SCRYFALL_BULK_DATA_URL = "https://api.scryfall.com/bulk-data"
SCRYFALL_HEADERS = {
    "User-Agent": "https://github.com/lotka/edhbudget.com",
    "Accept": "application/json",
}
PRICE_KEYS = ["usd", "usd_foil", "eur", "eur_foil"]
CARD_KEYS = ["set_name", "name", "id"]


def min_special(a, b):
    if pd.isna(a) and pd.isna(b):
        return np.nan
    if pd.isna(a):
        return b
    if pd.isna(b):
        return a

    return min(float(a), float(b))


def get_default_cards_metadata():
    bulk = requests.get(SCRYFALL_BULK_DATA_URL, headers=SCRYFALL_HEADERS).json()
    return next(entry for entry in bulk["data"] if entry["type"] == "default_cards")


def get_latest_loaded_datetime():
    return pd.read_gbq(
        """
        SELECT MAX(datetime) FROM `nifty-beast-realm.magic.scryfall-prices-v2`
        """,
        project_id=PROJECT_ID,
    ).values[0][0]


def card_has_paper_price(card):
    return any(card["prices"].values()) and "paper" in card["games"]


def normalize_scryfall_card(card, datetime):
    new_entry = {key: card.get(key) for key in CARD_KEYS}
    prices = card["prices"]

    for key in PRICE_KEYS:
        new_entry[f"price_{key}"] = float(prices[key]) if prices[key] is not None else np.nan

    new_entry["main_price_usd"] = min_special(prices["usd"], prices["usd_foil"])
    new_entry["main_price_eur"] = min_special(prices["eur"], prices["eur_foil"])
    new_entry["datetime"] = datetime
    return new_entry


def build_price_frame(cards, datetime):
    rows = [
        normalize_scryfall_card(card, datetime)
        for card in tqdm(cards)
        if card_has_paper_price(card)
    ]
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["main_price_usd"] = df["main_price_usd"].astype(float)
    df["main_price_eur"] = df["main_price_eur"].astype(float)
    return df


def post_webhook(webhook_url, content):
    if webhook_url:
        requests.post(webhook_url, json={"content": content})


def run(webhook_url, start_time):
    meta = get_default_cards_metadata()
    latest_scryfall_datetime = meta["updated_at"]

    print("Downloading data...")
    cards = requests.get(meta["download_uri"], headers=SCRYFALL_HEADERS, stream=True).json()

    newest = max(
        (card for card in cards if card_has_paper_price(card)),
        key=lambda card: card.get("released_at") or "",
    )

    if pd.to_datetime(latest_scryfall_datetime) <= pd.to_datetime(get_latest_loaded_datetime()):
        post_webhook(
            webhook_url,
            f"```Nothing to be done, prices are up to date.\n"
            f"Newest set: {newest['set_name']} ({newest['released_at']})```",
        )
        print("Nothing to be done")
        return

    print("Processing data...")
    df = build_price_frame(cards, latest_scryfall_datetime)

    print("Uploading data..")
    df.to_gbq(PRICE_TABLE, project_id=PROJECT_ID, if_exists="append")

    elapsed = int(time.time() - start_time)

    post_webhook(
        webhook_url,
        f"```Prices processed in {elapsed} second\n"
        f"Total cards: {len(df):,}\n"
        f"Unique cards: {len(df.name.unique()):,}\n"
        f"Unique sets: {len(df.set_name.unique()):,}\n"
        f"Newest set: {newest['set_name']} ({newest['released_at']})```",
    )


def main(_):
    webhook_url = os.getenv("WEBHOOK", None)
    start_time = time.time()
    try:
        run(webhook_url, start_time)
    except Exception:
        user_id = os.getenv("DISCORD_USER_ID", None)
        mention = f"<@{user_id}> " if user_id else ""
        post_webhook(
            webhook_url,
            f"{mention}scryfall-bq-update failed :rotating_light:\n"
            f"```{traceback.format_exc()[-1500:]}```",
        )
        raise
    return "OK"
