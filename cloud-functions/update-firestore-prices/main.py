import hashlib
import json
import os
import time
import traceback

import firebase_admin
import pandas as pd
import requests
from firebase_admin import credentials, firestore
from tqdm import tqdm


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "nifty-beast-realm"
FIRESTORE_COLLECTION_CARDS = os.getenv("FIRESTORE_COLLECTION_CARDS", "card-prices-v2")
PRICE_TABLE = os.getenv("PRICE_TABLE", "nifty-beast-realm.magic.scryfall-prices-v2")
SEASON_START = os.getenv("SEASON_START", "2026-01-01")
NEXT_SEASON_START = os.getenv("NEXT_SEASON_START", "2026-04-01")
BATCH_SIZE = 500


def safe_doc_id(name: str) -> str:
    return hashlib.md5(name.encode()).hexdigest()


def build_price_query():
    return f"""
    WITH season AS (
    SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_season FROM `{PRICE_TABLE}`
    WHERE TIMESTAMP('{SEASON_START}') <= datetime and datetime < TIMESTAMP('{NEXT_SEASON_START}') and main_price_usd is not null
    GROUP BY name, datetime
    ),
    season_new AS (
    SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_season_new FROM `{PRICE_TABLE}`
    WHERE TIMESTAMP('{NEXT_SEASON_START}') <= datetime and main_price_usd is not null
    GROUP BY name, datetime
    )
    SELECT name,
        AVG(price_season) as price_season,
        AVG(price_season_new) as price_season_new,
        IFNULL(AVG(price_season),AVG(price_season_new)) as price_season_combined,
    FROM season
    FULL OUTER JOIN season_new USING (name,datetime)
    GROUP BY name
    """


def initialize_firestore():
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})

    return firestore.client()


def price_document(row):
    return json.loads(row[["name", "price_season", "price_season_new", "price_season_combined"]].to_json())


def write_prices(db, df):
    for start in tqdm(range(0, df.shape[0], BATCH_SIZE)):
        batch = db.batch()
        for _, row in df.iloc[start:start + BATCH_SIZE].iterrows():
            card_ref = db.collection(FIRESTORE_COLLECTION_CARDS).document(safe_doc_id(row["name"]))
            batch.set(card_ref, price_document(row))

        batch.commit()


def post_webhook(webhook_url, content):
    if webhook_url:
        requests.post(webhook_url, json={"content": content})


def run(webhook_url, start_time):
    df = pd.read_gbq(build_price_query(), project_id=PROJECT_ID)
    db = initialize_firestore()
    write_prices(db, df)

    elapsed = int(time.time() - start_time)
    batches = (df.shape[0] + BATCH_SIZE - 1) // BATCH_SIZE
    priciest = df.loc[df["price_season_combined"].idxmax()]
    post_webhook(
        webhook_url,
        f"```Firestore prices updated in {elapsed} second\n"
        f"Total cards written: {len(df):,}\n"
        f"With current season price: {df['price_season'].notna().sum():,}\n"
        f"With new season price: {df['price_season_new'].notna().sum():,}\n"
        f"Median combined price: ${df['price_season_combined'].median():,.2f}\n"
        f"Most expensive: {priciest['name']} (${priciest['price_season_combined']:,.2f})\n"
        f"Batches committed: {batches:,}```",
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
            f"{mention}update-firestore-prices failed :rotating_light:\n"
            f"```{traceback.format_exc()[-1500:]}```",
        )
        raise
    return "OK"
