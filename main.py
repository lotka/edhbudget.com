import concurrent.futures

import flask
from google.cloud import bigquery


from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
import pandas as pd
import requests

class SubmitForm(FlaskForm):
    url = StringField('url', validators=[DataRequired()])
    submit = SubmitField('Submit')


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

@app.route('/submit', methods=['GET', 'POST'])
def login():
    form = SubmitForm()
    if form.validate_on_submit():
        print("wtf!?")
        archidekt_id = form.url.data.split('/')[-1].split('#')[0]
        # new_deck = price_archidekt("https://archidekt.com/api/decks/{}/".format(archidekt_id))
        print('BQ: deck_ids post')
        pd.DataFrame([{'id' : archidekt_id}]).to_gbq('magic.deck_ids',project_id='nifty-beast-realm',if_exists='append')
        return flask.redirect(flask.url_for('main'))
    return flask.render_template('submit.html',  title='Sign In', form=form)


@app.route("/", methods=['GET', 'POST'])
def main():

    form = SubmitForm()
    if form.validate_on_submit():
        print("wtf!?")
        archidekt_id = form.url.data.split('/')[-1].split('#')[0]
        if requests.get('https://archidekt.com/api/decks/{}/'.format(archidekt_id)).status_code != 200:
            flask.flash('Bad archidekt URL! {}'.format(form.url.data))
            return flask.redirect(flask.url_for('main'))
        else:
            pd.DataFrame([{'id' : archidekt_id}]).to_gbq('magic.deck_ids',project_id='nifty-beast-realm',if_exists='append')
            return flask.redirect(flask.url_for('main'))

    print('BQ: deck_ids get')
    deck_ids = pd.read_gbq("""
    SELECT * FROM `nifty-beast-realm.magic.deck_ids` LIMIT 1
    """,project_id="nifty-beast-realm")
    prices = []
    for id in deck_ids['id'].values:
        prices.append(price_archidekt("https://archidekt.com/api/decks/{}/".format(id)))

    df = pd.DataFrame(prices).sort_values(by='deck_price',ascending=False)
    return flask.render_template("query_result.html", results=df.values,form=form)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
