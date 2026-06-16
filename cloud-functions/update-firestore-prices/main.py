import hashlib
import json
import os
import time
import traceback
from datetime import date

import firebase_admin
import pandas as pd
import requests
from firebase_admin import credentials, firestore
from tqdm import tqdm


PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "nifty-beast-realm"
FIRESTORE_COLLECTION_CARDS = os.getenv("FIRESTORE_COLLECTION_CARDS", "card-prices-v2")
PRICE_TABLE = os.getenv("PRICE_TABLE", "nifty-beast-realm.magic.scryfall-prices-v2")
BATCH_SIZE = 500


def season_bounds(today=None):
    """Return (previous_season_start, current_season_start) as ISO date strings.

    Seasons are 4-month blocks starting Jan 1, May 1, and Sep 1. The query treats
    [previous_start, current_start) as the completed "season" and everything from
    current_start onward as the live "next season", so we return the previous and
    current season starts respectively.
    """
    today = today or date.today()
    if today.month >= 9:
        current = date(today.year, 9, 1)
        previous = date(today.year, 5, 1)
    elif today.month >= 5:
        current = date(today.year, 5, 1)
        previous = date(today.year, 1, 1)
    else:
        current = date(today.year, 1, 1)
        previous = date(today.year - 1, 9, 1)

    return previous.isoformat(), current.isoformat()


_default_season_start, _default_next_season_start = season_bounds()
SEASON_START = os.getenv("SEASON_START", _default_season_start)
NEXT_SEASON_START = os.getenv("NEXT_SEASON_START", _default_next_season_start)


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


def post_webhook(webhook_url, content, allowed_mentions=None):
    if webhook_url:
        payload = {"content": content}
        if allowed_mentions is not None:
            payload["allowed_mentions"] = allowed_mentions
        requests.post(webhook_url, json=payload)


def season_just_rolled_over(today=None):
    # The job runs daily, so a rollover is exactly the boundary day where the
    # current season starts on today's date.
    today = today or date.today()
    return NEXT_SEASON_START == today.isoformat()


def post_season_rollover(webhook_url):
    role_id = os.getenv("MAGIC_ROLE_ID", None)
    mention = f"<@&{role_id}> " if role_id else ""
    post_webhook(
        webhook_url,
        f"{mention}:tada: New season rolled over! Now scoring the {NEXT_SEASON_START} season; "
        f"the {SEASON_START} season is locked in as the previous season price.",
        allowed_mentions={"parse": ["roles"]},
    )


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
        f"Total value of all cards: ${df['price_season_combined'].sum():,.2f}\n"
        f"Most expensive: {priciest['name']} (${priciest['price_season_combined']:,.2f})\n"
        f"Batches committed: {batches:,}```",
    )

    if season_just_rolled_over():
        post_season_rollover(webhook_url)


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
