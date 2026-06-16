import hashlib
import json
import os

import firebase_admin
import pandas as pd
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


def main(_):
    df = pd.read_gbq(build_price_query(), project_id=PROJECT_ID)
    db = initialize_firestore()
    write_prices(db, df)
    return "OK"
