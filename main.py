import concurrent.futures

import flask
from google.cloud import bigquery

import archidekt
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
import pandas as pd
import requests

class SubmitForm(FlaskForm):
    url = StringField('url', validators=[DataRequired()])
    submit = SubmitField('Submit')


import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# Use the application default credentials
cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
  'projectId': "nifty-beast-realm",
})

db = firestore.client()


def price_archidekt(url):
    data = requests.get(url=url).json()

    commander = None
    for card in data['cards']:
        if 'Commander' in card['categories'] and 'Planeswalker' in card['card']['oracleCard']['types']:
            commander = card['card']['oracleCard']['name']

    cards = []
    for card in data['cards']:
        if 'Basic' not in card['card']['oracleCard']['superTypes'] and 'Maybeboard' not in card['categories']:
            cards.append(card['card']['oracleCard']['name'])
        where_in_statement = '['
    for card in cards:
        where_in_statement += "\"{}\", ".format(card)
    where_in_statement = where_in_statement[:-2] + ']'

    q = """
    WITH historical AS (
    SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price FROM `nifty-beast-realm.magic.scryfall-prices`
    WHERE name IN UNNEST({})
    GROUP BY name, datetime
    )
    SELECT name,AVG(price) as price FROM historical
    GROUP BY name
    """.format(where_in_statement)
    print('BQ: get prices')
    historical = pd.read_gbq(q,project_id="nifty-beast-realm")

    return {'name': data['name'],
            'owner': data['owner']['username'],
            'url' : url.replace('/api',''),
            'commander' : commander,
            'commander_price' : round(historical['price'][historical.name == commander].sum(),2),
            'deck_price' : round(historical['price'][historical.name != commander].sum(),2),
            'cards' : len(historical),
            'free_cards' : (historical['price'] == 0).sum()
            }

app = flask.Flask(__name__)
app.config['SECRET_KEY'] = 'you-will-never-guess'
bigquery_client = bigquery.Client()

# @app.route('/submit', methods=['GET', 'POST'])
# def login():
#     form = SubmitForm()
#     if form.validate_on_submit():
#         print("wtf!?")
#         archidekt_id = form.url.data.split('/')[-1].split('#')[0]
#         # new_deck = price_archidekt("https://archidekt.com/api/decks/{}/".format(archidekt_id))
#         print('BQ: deck_ids post')
#         doc_ref = db.collection(u'deck-ids').document(archidekt_id)
#         doc_ref.set({
#             u'modified': str(pd.datetime.today()),
#             u'data': {'complex': 4.2},
#         })
#         pd.DataFrame([{'id' : archidekt_id}]).to_gbq('magic.deck_ids',project_id='nifty-beast-realm',if_exists='append')
#         return flask.redirect(flask.url_for('main'))
#     return flask.render_template('submit.html',  title='Sign In', form=form)


@app.route("/", methods=['GET', 'POST'])
def main():

    form = SubmitForm()
    if form.validate_on_submit():
        print("wtf!?")
        archidekt_id = form.url.data.split('/')[-1].split('#')[0]
        deck_request = requests.get('https://archidekt.com/api/decks/{}/'.format(archidekt_id))
        if deck_request.status_code != 200:
            flask.flash('Bad archidekt URL! {}'.format(form.url.data))
            return flask.redirect(flask.url_for('main'))
        else:
            doc_ref = db.collection(u'deck-ids').document(archidekt_id)
            data = deck_request.json()
            doc_ref.set({
                u'modified': str(pd.datetime.today()),
                # u'cards': archidekt.get_cards_in_archidekt(data),
                # u'commander': archidekt.get_commander_in_archidekt(data),
            })
            # pd.DataFrame([{'id' : archidekt_id}]).to_gbq('magic.deck_ids',project_id='nifty-beast-realm',if_exists='append')
            return flask.redirect(flask.url_for('main'))
    else:
        print('FS: deck_ids get')
        deck_ids_ref = db.collection(u'deck-ids')
        prices = []
        for id in list(map(lambda x: x.id, deck_ids_ref.stream())):
            prices.append(price_archidekt("https://archidekt.com/api/decks/{}/".format(id)))
        if len(prices) > 0:
            df = pd.DataFrame(prices).sort_values(by='deck_price',ascending=False)
        else:
            df = pd.DataFrame(prices)
        return flask.render_template("query_result.html", results=df.values,form=form)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
