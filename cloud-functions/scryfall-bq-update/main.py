import numpy as np
import requests
from tqdm import tqdm
import pandas as pd
import os
import time

def min_special(a,b):
    if pd.isna(a) and pd.isna(b):
        return np.nan
    if pd.isna(a):
        return b
    elif pd.isna(b):
        return a
    else:
        return min(float(a),float(b))

def main(_):
    webhook_url = os.getenv('WEBHOOK',None)
    start_time = time.time()
    bulk = requests.get('https://api.scryfall.com/bulk-data').json()
    meta = list(filter(lambda x : x['type'] == 'default_cards',bulk['data']))[0]
    today = meta['updated_at']

    max_datetime = pd.read_gbq("""
    SELECT MAX(datetime) FROM `nifty-beast-realm.magic.scryfall-prices`
    """,project_id='nifty-beast-realm').values[0][0]

    if pd.to_datetime(today) > pd.to_datetime(max_datetime):
        print('Downloading data...')
        data = requests.get(meta['download_uri'], stream=True).json()

        new_data = []
        needed_keys = ['set_name','name','id']

        print('Processing data...')
        for i in tqdm(range(len(data))):
            new_entry = {}
            # Copy over the data
            for k in needed_keys:
                if k in data[i]:
                    new_entry[k] = data[i][k]
                else:
                    new_entry[k] = None
            # Expand prices
            prices = data[i]['prices']
            for k in ["usd", "usd_foil", "eur", "eur_foil"]:
                if  prices[k] is not None:
                    new_entry['price_'+k] = float(prices[k])
                else:
                    new_entry['price_'+k] = np.nan
            new_entry['main_price_usd'] = min_special(prices['usd'],prices['usd_foil'])
            new_entry['main_price_eur'] = min_special(prices['eur'],prices['eur_foil'])
            new_entry['datetime'] = today
            # Remove funny cards
            if any(prices.values()) and 'paper' in data[i]['games']:
                new_data.append(new_entry)
        
        df = pd.DataFrame(new_data)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['main_price_usd'] = df['main_price_usd'].astype(float)
        df['main_price_eur'] = df['main_price_eur'].astype(float)
        print('Uploading data..')
        df.to_gbq('magic.scryfall-prices',project_id='nifty-beast-realm',if_exists='append')
        end_time = time.time()

        if webhook_url:
            logging_str = f'```Prices processed in {int(end_time - start_time)} second\nTotal cards: {len(df):,}\nUnique cards: {len(df.name.unique()):,}\nUnique sets: {len(df.set_name.unique()):,}```'
            requests.post(webhook_url, json={"content": logging_str})
    else:
        if webhook_url:
            requests.post(webhook_url, json={"content": f"Prices are up to date."})
        print('Nothing to be done')

    return 'OK'