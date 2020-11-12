# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START gae_python38_bigquery]
import concurrent.futures

import flask
from google.cloud import bigquery


from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField
from wtforms.validators import DataRequired
import datetime
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

def example_prices():
    prices = []
    for id in [632823,858847,875830,899287,626928,754166,623693]:
        prices.append(price_archidekt("https://archidekt.com/api/decks/{}/".format(id)))
    return pd.DataFrame(prices)


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
    SELECT * FROM `nifty-beast-realm.magic.deck_ids` LIMIT 1000
    """,project_id="nifty-beast-realm")
    prices = []
    for id in deck_ids['id'].values:
        prices.append(price_archidekt("https://archidekt.com/api/decks/{}/".format(id)))

    df = pd.DataFrame(prices).sort_values(by='deck_price',ascending=False)
    return flask.render_template("query_result.html", results=df.values,form=form)

@app.route("/results")
def results():
    project_id = flask.request.args.get("project_id")
    job_id = flask.request.args.get("job_id")
    location = flask.request.args.get("location")

    query_job = bigquery_client.get_job(
        job_id,
        project=project_id,
        location=location,
    )

    try:
        # Set a timeout because queries could take longer than one minute.
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("query_result.html", results=results)


if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
# [END gae_python38_bigquery]
