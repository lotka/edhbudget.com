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
        if 'Commander' in card['categories'] and 'Planeswalker' in card['card']['oracleCard']['types']:
            commander = card['card']['oracleCard']['name']

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
        WHERE name IN UNNEST({})
        GROUP BY name, datetime
        ),
        shifted_historical AS (
        SELECT name,datetime,min(CAST(main_price_usd as FLOAT64)) as price_lag FROM `nifty-beast-realm.magic.scryfall-prices`
        WHERE name IN UNNEST({}) and datetime < TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK))
        GROUP BY name, datetime
        )
        SELECT name,AVG(price) as price, AVG(price_lag) as price_lag, AVG(price) - AVG(price_lag) as change FROM shifted_historical
        LEFT JOIN historical USING (name)
        GROUP BY name
        """.format(where_in_statement,where_in_statement)
        print('BQ: get prices')
        historical = pd.read_gbq(q, project_id="nifty-beast-realm")

        return {'name': data['name'],
                'owner': data['owner']['username'],
                'url': url.replace('/api', ''),
                'commander': commander,
                'commander_price': round(historical['price'][historical.name == commander].sum(), 2),
                'deck_price': round(historical['price'][historical.name != commander].sum(), 2),
                'deck_price_change': round(historical['change'][historical.name != commander].sum(), 2),
                'cards': len(historical),
                'free_cards': int((historical['price'] == 0).sum()),
                'id': data['id'],
                'modified': str(datetime.datetime.today()),
                }
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
bigquery_client = bigquery.Client()


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
        doc_ref = db.collection(u'deck-ids').document(archidekt_id)
        result = calculate_price_archidekt(deck_request.json(),url)
        doc_ref.set(result)
        return result


@app.route("/", methods=['GET', 'POST'])
def main():
    form = SubmitForm()
    if form.validate_on_submit():
        if not update_deck(form.url.data.split('/')[-1].split('#')[0]):
            flask.flash('Bad archidekt URL! {}'.format(form.url.data))
        return flask.redirect(flask.url_for('main'))
    else:
        print('FS: deck_ids get')
        deck_ids_ref = db.collection(u'deck-ids').order_by('deck_price',direction=firestore.Query.DESCENDING)
        res = []
        for doc in deck_ids_ref.stream():
            res.append(doc.to_dict())
        return flask.render_template("index.html",
                                     title='Oathy Budgets',
                                     results=res,
                                     form=form,
                                     update_form=UpdateForm())


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
