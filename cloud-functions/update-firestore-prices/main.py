import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import firebase_admin
import pandas as pd
import json
import re

FIRESTORE_COLLECTION_CARDS = 'card-prices'

import re
import hashlib

def safe_doc_id(name: str) -> str:
    if not isinstance(name, str):
        name = str(name)

    # Replace slashes first
    name = name.replace('/', '-')

    # Remove problematic chars
    name = re.sub(r'[^\w\s\-]', '', name)

    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()

    # Replace spaces with underscores
    name = name.replace(' ', '_')

    if not name or name in {'.', '..'} or re.fullmatch(r'_+', name):
        # fallback to deterministic hash
        name = hashlib.md5(name.encode()).hexdigest()

    return name

def main(_):
    q = """
    WITH season AS (
    SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_season FROM `nifty-beast-realm.magic.scryfall-prices`
    WHERE TIMESTAMP('2026-01-01') <= datetime and datetime < TIMESTAMP('2026-04-01') and main_price_usd is not null
    GROUP BY name, datetime
    ),
    season_new AS (
    SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_season_new FROM `nifty-beast-realm.magic.scryfall-prices`
    WHERE TIMESTAMP('2026-04-01') <= datetime and main_price_usd is not null
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

    df = pd.read_gbq(q)
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {
            'projectId': "nifty-beast-realm",
        })
    
    db = firestore.client()

    from tqdm import tqdm

    debug = []
    N = round(df.shape[0]/500)+1
    for i in tqdm(range(N)):
        batch = db.batch()
        for j in range(500):
            k = i*500 + j
            if k < df.shape[0]:
                document_id = safe_doc_id(df.iloc[k]['name'])
                card_ref = db.collection(FIRESTORE_COLLECTION_CARDS).document(document_id)
                data = json.loads(df[['name','price_season', 'price_season_new','price_season_combined']].iloc[k].to_json())
                batch.set(card_ref, data)
                debug.append(k)

        # Commit the batch
        batch.commit()
    
    return 'OK'