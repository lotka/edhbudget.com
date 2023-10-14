from firebase_admin import credentials
from firebase_admin import firestore
import firebase_admin
import pandas as pd
import json

def main(_):
    q = """
    WITH season AS (
    SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_season FROM `nifty-beast-realm.magic.scryfall-prices`
    WHERE TIMESTAMP('2023-09-01') <= datetime and datetime < TIMESTAMP('2024-01-01') and main_price_usd is not null
    GROUP BY name, datetime
    ),
    season_new AS (
    SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_season_new FROM `nifty-beast-realm.magic.scryfall-prices`
    WHERE TIMESTAMP('2023-09-01') <= datetime and main_price_usd is not null
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

    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {
        'projectId': "nifty-beast-realm",
    })

    db = firestore.client()

    FIRESTORE_COLLECTION_CARDS = 'card-prices'
    db = firestore.client()
    batch = db.batch()

    from tqdm import tqdm

    debug = []
    N = round(df.shape[0]/500)+1
    for i in tqdm(range(N)):
        for j in range(500):
            k = i*500 + j
            if k < df.shape[0]:
                card_ref = db.collection(FIRESTORE_COLLECTION_CARDS).document(df.iloc[k]['name'].replace('//','---'))
                data = json.loads(df[['name','price_season', 'price_season_new','price_season_combined']].iloc[k].to_json())
                batch.set(card_ref, data)
                debug.append(k)

        # Commit the batch
        batch.commit()
