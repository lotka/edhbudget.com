import json
import flask
import pandas as pd
import requests
import datetime
import firebase_admin
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from google.cloud import bigquery
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from firebase_admin import credentials
from firebase_admin import firestore
from flask import request
from socket import gethostname

PRICE_PERIOD = 12
if gethostname() == 'LT40408':
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

        q = """
        WITH historical AS (
        SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price FROM `nifty-beast-realm.magic.scryfall-prices`
        WHERE name IN UNNEST({cards})
        and TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL {period} MONTH)) < datetime 
        GROUP BY name, datetime
        ),
        shifted_historical AS (
        SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_lag FROM `nifty-beast-realm.magic.scryfall-prices`
        WHERE name IN UNNEST({cards}) and datetime < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
        and TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1+{period} MONTH)) < datetime 
        GROUP BY name, datetime
        ),
        season AS (
        SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_season FROM `nifty-beast-realm.magic.scryfall-prices`
        WHERE name IN UNNEST({cards})
        and TIMESTAMP('2021-09-01') <= datetime and datetime < TIMESTAMP('2022-01-01')
        GROUP BY name, datetime
        ),
        season_new AS (
        SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_season_new FROM `nifty-beast-realm.magic.scryfall-prices`
        WHERE name IN UNNEST({cards})
        and TIMESTAMP('2022-01-01') <= datetime and datetime < TIMESTAMP('2022-03-09')
        GROUP BY name, datetime
        )
        SELECT name,
               AVG(price) as price,
               AVG(price_lag) as price_lag,
               IFNULL(AVG(price_season),AVG(price_season_new)) as price_season,
               AVG(price) - AVG(price_lag) as change 
        FROM shifted_historical
        LEFT JOIN historical USING (name,datetime)
        LEFT JOIN season USING (name,datetime)
        LEFT JOIN season_new USING (name,datetime)
        GROUP BY name
        """.format(cards=where_in_statement,period=PRICE_PERIOD)
        print(q)
        print('BQ: get prices')
        historical = pd.read_gbq(q, project_id="nifty-beast-realm")
        price_list = historical[['name','price','price_lag','price_season']].sort_values(
            by='price', ascending=False)
        price_list['price'] = price_list['price'].round(2)
        price_list['price_lag'] = price_list['price_lag'].round(2)
        price_list['price_season'] = price_list['price_season'].round(2)
        price_list = price_list.round(2).values.tolist()
        flat_price_list = []
        for value in price_list:
            flat_price_list.append(value[0])
            flat_price_list.append(value[1])
        

        res =  {'name': data['name'],
                'owner': data['owner']['username'],
                'url': url.replace('/api', ''),
                'commander': commander,
                'cards': len(historical),
                'commander_price' : round(historical['price'][historical.name == commander].sum(), 2),
                'free_cards': int((historical['price'] == 0).sum()),
                'id': data['id'],
                'modified': str(datetime.datetime.today()),
                'price_list': flat_price_list,
                'deckFormat': deckFormat,
                'deck_price_season': round(historical['price_season'].sum(), 2)
                }
        if deckFormat == 'oathbreaker':
            res['deck_price'] = round(historical['price'][historical.name != commander].sum(), 2)
            res['deck_price_change'] = round(historical['change'][historical.name != commander].sum(), 2)
        else:
            res['deck_price'] = round(historical['price'].sum(), 2)
            res['deck_price_change'] = round(historical['change'].sum(), 2)
        return res
    else:
        return {'name': data['name'],
                'owner': data['owner']['username'],
                'url': url.replace('/api', ''),
                'commander': commander,
                'commander_price': 0,
                'deck_price': 0.0000000000000001,
                'deck_price_change': 0,
                'cards': 0,
                'free_cards': 0,
                'id': data['id'],
                'modified': str(datetime.datetime.today()),
                }


def price_archidekt(url):
    data = requests.get(url=url).json()
    return calculate_price_archidekt(data,url)

app = flask.Flask(__name__)
bootstrap = Bootstrap(app)
app.config['SECRET_KEY'] = 'you-will-never-guess'
bigquery_client = bigquery.Client(project='nifty-beast-realm')


@app.route('/price_list', methods=['GET'])
def api_filter():
    doc_ref = db.collection(FIRESTORE_COLLECTION).document(request.args['archidekt_id'])
    data = doc_ref.get().to_dict()['price_list']
    res ="""<style>
        table, th, td {
        border: 0px solid black;
        font-size: 12px;
        font-weight: normal;
        font-family: Arial, Helvetica, sans-serif;
        }
        </style>
        <table>"""
    for i in range(0,len(data),2):
        res += '<tr> <th>{}</th> <th><b>{}</b></th> <th>{}</th> </tr>'.format(
            i //2 + 1, data[i], data[i+1])
    res += '</table>'
    return res
    


@app.route('/update_deck_id', methods=['POST'])
def update_deck_id():
    print('start')
    data = update_deck(flask.request.form['id'])
    print('end')
    return flask.jsonify(data)


def update_deck(archidekt_id):
    url = 'https://archidekt.com/api/decks/{}/'.format(archidekt_id)
    deck_request = requests.get(url)
    if deck_request.status_code != 200:
        return False
    else:
        doc_ref = db.collection(FIRESTORE_COLLECTION).document(archidekt_id)
        result = calculate_price_archidekt(deck_request.json(),url)
        doc_ref.set(result)
        return result


def main_page(deckFormat,budget):
    form = SubmitForm()
    if form.validate_on_submit():
        if not update_deck(form.url.data.split('/')[-1].split('#')[0]):
            flask.flash('Bad archidekt URL! {}'.format(form.url.data))
        return flask.redirect(flask.url_for(deckFormat))
    else:
        print('FS: deck_ids get')
        deck_ids_ref = db.collection(FIRESTORE_COLLECTION).order_by('owner', direction=firestore.Query.DESCENDING)
        res = []
        average_price = 0
        for doc in deck_ids_ref.stream():
            doc = doc.to_dict()
            if doc['deckFormat'] != deckFormat:
                continue
            average_price += doc['deck_price']
            res.append(doc)
            if 'deck_price_season' not in doc:
                doc['deck_price_season'] = 0.00
        if len(res) > 0:
            average_price = average_price/float(len(res))
        else:
            average_price = 0
        return flask.render_template("index.html",
                                     title=deckFormat,
                                     budget=budget,
                                     average_price=round(average_price,2),
                                     price_period=PRICE_PERIOD,
                                     results=res,
                                     form=form,
                                     update_form=UpdateForm())


@app.route("/oathbreaker", methods=['GET', 'POST'])
def oathbreaker():
    return main_page('oathbreaker',budget=35)


@app.route("/", methods=['GET', 'POST'])
def main():
    return main_page('edh', budget=60)

@app.route("/edh", methods=['GET', 'POST'])
def edh():
    return main_page('edh', budget=60)


@app.route("/deck", methods=['GET', 'POST'])
def deck():
    form = SubmitForm()
    if form.validate_on_submit():
        if not update_deck(form.url.data.split('/')[-1].split('#')[0]):
            flask.flash('Bad archidekt URL! {}'.format(form.url.data))
        return flask.redirect(flask.url_for('main'))
    else:
        doc_ref = db.collection(FIRESTORE_COLLECTION).document(request.args['archidekt_id'])
        data = doc_ref.get().to_dict()['price_list']
        print(data)
        res = []
        for i in range(0, len(data), 3):
            a,b,c = data[i],float(data[i+1]), float(data[i+2])
            res.append([data[i],
                        data[i+1], data[i+2],
                        100*round((data[i+1] - data[i+2])/data[i+1], 2)])
        return flask.render_template("deck.html",
                                     title='Oathy Budgets',
                                     results=res,
                                     form=form,
                                     update_form=UpdateForm())


if __name__ == "__main__":
    
    app.run(host="0.0.0.0", port=8080, debug=True)
