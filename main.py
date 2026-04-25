import flask
import pandas as pd
import requests
import datetime
import logging
import os
from zoneinfo import ZoneInfo
import firebase_admin
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from google.cloud import bigquery
from google.cloud import secretmanager
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from firebase_admin import credentials
from firebase_admin import firestore
from flask import request
import sys
from functools import lru_cache
from urllib.parse import urlparse
import re


DEBUG = len(sys.argv) > 1 and sys.argv[1] == 'dev'

if DEBUG:
    FIRESTORE_COLLECTION = u'deck-ids-dev'
else:
    FIRESTORE_COLLECTION = u'deck-ids'

class SubmitForm(FlaskForm):
    url = StringField('url', validators=[DataRequired()])
    submit = SubmitField('Submit')


class UpdateForm(FlaskForm):
    submit = SubmitField('Update Prices')

# Use the application default credentials
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
    'projectId': "nifty-beast-realm",
})

db = firestore.client()
DISCORD_WEBHOOK_SECRET_ID = 'discord-webhook-url'

def remove_nan(x):
    if pd.isnull(x):
        return 'NEW CARD'
    else:
        return x

@lru_cache(maxsize=1)
def get_discord_webhook_url():
    project_id = (
        os.getenv('GOOGLE_CLOUD_PROJECT')
        or os.getenv('GCP_PROJECT')
        or firebase_admin.get_app().project_id
    )
    secret_name = f'projects/{project_id}/secrets/{DISCORD_WEBHOOK_SECRET_ID}/versions/latest'
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8").strip()

def notify_new_deck(result):
    try:
        webhook_url = get_discord_webhook_url()
    except Exception:
        logging.exception('Failed to load Discord webhook secret')
        return

    if not webhook_url:
        return

    message = (
        f"New deck added: **{result['name']}** by **{result['owner']}**\n"
        f"{result['url']}\n"
        f"Format: `{result['deckFormat']}` | Price: `${result['deck_price_season']}`"
    )

    try:
        requests.post(webhook_url, json={"content": message}, timeout=10)
    except Exception:
        logging.exception('Failed to send Discord webhook notification')

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

def calculate_price_archidekt(data,url):
    commander = 'Commander not found'
    for card in data['cards']:
        if 'Commander' in card['categories'] and ('Planeswalker' in card['card']['oracleCard']['types'] or 'Creature' in card['card']['oracleCard']['types']):
            commander = card['card']['oracleCard']['name']

    if data['deckFormat'] == 14:
        deckFormat = 'oathbreaker'
    elif data['deckFormat'] == 3:
        deckFormat = 'edh'
    else:
        deckFormat = 'other'

    cards = []
    if len(data['cards']) > 0:
        for card in data['cards']:
            if 'Basic' not in card['card']['oracleCard']['superTypes'] and 'Maybeboard' not in card['categories'] and 'Sideboard' not in card['categories']:
                cards.append(card['card']['oracleCard']['name'])
            where_in_statement = '['
        for card in cards:
            where_in_statement += "\"{}\", ".format(card)
        where_in_statement = where_in_statement[:-2] + ']'

        card_prices = []
        for card in cards:
            doc_ref = db.collection(u'card-prices').document(safe_doc_id(card))
            doc = doc_ref.get()
            if doc.exists:
                card_prices.append(doc.to_dict())
            else:
                flask.flash('Missing {}, contact the admin 💀'.format(card))
                print(card,'MISSING!!')
            
        historical = pd.DataFrame(card_prices)

        price_list = historical[['name','price_season','price_season_new','price_season_combined']].sort_values(
            by='price_season_combined', ascending=False)
        price_list['price_season'] = price_list['price_season'].astype(float)
        price_list['price_season'] = price_list['price_season'].round(2)
        price_list['price_season_new'] = price_list['price_season_new'].round(2)
        price_list = price_list.round(2).values.tolist()
        flat_price_list = []

        for value in price_list:
            flat_price_list.append(remove_nan(value[0]))
            flat_price_list.append(remove_nan(value[1]))
            flat_price_list.append(remove_nan(value[2]))
        

        res =  {'name': data['name'],
                'owner': data['owner']['username'],
                'url': url.replace('/api', ''),
                'commander': commander,
                'cards': len(historical),
                'commander_price' : round(historical['price_season_combined'][historical.name == commander].sum(), 2),
                'free_cards': int((historical['price_season_combined'] == 0).sum()),
                'id': data['id'],
                'modified': str(datetime.datetime.today()),
                'price_list': flat_price_list,
                'deckFormat': deckFormat,
                'deck_price_season': round(historical['price_season_combined'].sum(), 2),
                'deck_price_season_new': round(historical['price_season_new'].sum(), 2),
                'deck_price': round(historical['price_season_combined'].sum(), 2),
                'deck_price_change': 0
                }
        return res
    else:
        return {'name': data['name'],
                'owner': data['owner']['username'],
                'url': url.replace('/api', ''),
                'commander': commander,
                'cards': 0,
                'commander_price': 0,
                'free_cards': 0,
                'id': data['id'],
                'modified': str(datetime.datetime.today()),
                'price_list' : [],
                'deckFormat' : 'n/a',
                'deck_price_season': 0.0000000000000001,
                'deck_price_season_new': 0.0000000000000001,
                }


# def price_archidekt(url):
#     data = requests.get(url=url).json()
#     return calculate_price_archidekt(data,url)

app = flask.Flask(__name__)
bootstrap = Bootstrap(app)
app.config['SECRET_KEY'] = 'you-will-never-guess'
bigquery_client = bigquery.Client(project='nifty-beast-realm')

@app.route('/update_deck_id', methods=['POST'])
def update_deck_id():
    data = update_deck(flask.request.form['id'])
    return flask.jsonify(data)

def update_deck(archidekt_id):
    url = 'https://archidekt.com/api/decks/{}/'.format(archidekt_id)
    deck_request = requests.get(url)
    if deck_request.status_code != 200:
        db.collection(FIRESTORE_COLLECTION).document(archidekt_id).delete()
        return False
    else:
        doc_ref = db.collection(FIRESTORE_COLLECTION).document(archidekt_id)
        is_new_deck = not doc_ref.get().exists
        result = calculate_price_archidekt(deck_request.json(),url)
        doc_ref.set(result)
        if is_new_deck:
            notify_new_deck(result)
        return result
    
def parse_url(url):
    o = urlparse(url)
    split = o.path.split('/')
    if len(split) < 3:
        return False
    archidekt_id = split[2]
    if not archidekt_id.isdigit():
        return False
    return archidekt_id

def main_page(deckFormat,budget,experimental=False,request=None):
    form = SubmitForm()
    owners = []
    if form.validate_on_submit():
        archidekt_id = parse_url(form.url.data)
        if archidekt_id:
            if update_deck(archidekt_id):
                return flask.redirect(flask.url_for(deckFormat))
            else:
                flask.flash('Failed to update deck URL: {}'.format(form.url.data))
        else:
            flask.flash('Bad archidekt URL: {}'.format(form.url.data))

        return flask.redirect(flask.url_for(deckFormat))
    else:
        deck_ids_ref = db.collection(FIRESTORE_COLLECTION).order_by('modified', direction=firestore.Query.DESCENDING)
        res = []
        average_price = 0
        owner_filter=None
        if request:
            if 'owner' in request.args:
                owner_filter = request.args['owner']
        for doc in deck_ids_ref.stream():
            doc = doc.to_dict()
            owners.append(doc['owner'])
            if owner_filter and doc['owner'] != owner_filter:
                continue
            if doc['deckFormat'] != deckFormat:
                continue
            if "[P]" not in doc['name'] and not experimental:
                continue
            if not experimental:
                doc['name'] = doc['name'].replace("[P]", "")

            # Fix timezone to UK
            doc['modified'] = pd.to_datetime(doc['modified'],utc=True).tz_convert('Europe/London').strftime('%Y-%m-%d %H:%M')
            
            average_price += doc['deck_price_season']
            res.append(doc)
            if 'deck_price_season' not in doc:
                doc['deck_price_season'] = 0.00
        if len(res) > 0:
            average_price = average_price/float(len(res))
        else:
            average_price = 0
        return flask.render_template("index.html",
                                     title=deckFormat,
                                     experimental=experimental,
                                     budget=budget,
                                     average_price=round(average_price,2),
                                     results=res,
                                     form=form,
                                     owners=list(set(owners)),
                                     update_form=UpdateForm())

@app.route("/", methods=['GET', 'POST'])
def main():
    return main_page('edh', budget=60, experimental=False,request=request)

@app.route("/beta", methods=['GET', 'POST'])
def beta():
    return flask.render_template("beta.html",
                                 title='edhbudget',
                                 form=SubmitForm())

@app.route("/faq", methods=['GET', 'POST'])
def faq():
    return flask.render_template("faq.html",
                                 title='faq')

@app.route("/oathbreaker", methods=['GET', 'POST'])
def oathbreaker():
    return main_page('oathbreaker',budget=35, experimental=True)

@app.route("/edh", methods=['GET', 'POST'])
def edh():
    return main_page('edh', budget=60, experimental=False,request=request)

@app.route("/edh-experimental", methods=['GET', 'POST'])
def edh_experimental():
    return main_page('edh', budget=60,experimental=True,request=request)

@app.route("/deck", methods=['GET', 'POST'])
def deck():
    form = SubmitForm()
    if form.validate_on_submit():
        if not update_deck(form.url.data.split('/')[-1].split('#')[0]):
            flask.flash('Bad archidekt URL! {}'.format(form.url.data))
        return flask.redirect(flask.url_for('main'))
    else:
        doc_ref = db.collection(FIRESTORE_COLLECTION).document(request.args['archidekt_id'])
        data = doc_ref.get().to_dict()
        price_list = data['price_list']
        res = []
        for i in range(0, len(price_list), 3):
            res.append([price_list[i],
                        price_list[i+1],
                        price_list[i+2]])
        return flask.render_template("deck.html",
                                     title='edhbudget',
                                     results=res,
                                     deck_name=data['name'],
                                     form=form,
                                     update_form=UpdateForm())

@app.route("/robots.txt")
def robots_dot_txt():
    return "User-agent: *\nDisallow: /"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=DEBUG)
