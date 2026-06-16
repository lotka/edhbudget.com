import flask
from flask import request
from flask_bootstrap import Bootstrap

from archidekt import parse_archidekt_id
from config import DEBUG, EDH_BUDGET, OATHBREAKER_BUDGET, SECRET_KEY
from deck_service import get_deck, list_decks, price_rows_for_template, update_deck
from forms import SubmitForm, UpdateForm
from price_history import get_card_price_history, get_deck_price_history


app = flask.Flask(__name__)
bootstrap = Bootstrap(app)
app.config["SECRET_KEY"] = SECRET_KEY


@app.route("/update_deck_id", methods=["POST"])
def update_deck_id():
    data = update_deck(flask.request.form["id"])
    if not data or "error" in data:
        return flask.jsonify(data), 400

    return flask.jsonify(data)


def render_deck_list(deck_format, budget, experimental=False):
    form = SubmitForm()
    if form.validate_on_submit():
        archidekt_id = parse_archidekt_id(form.url.data)
        if archidekt_id:
            result = update_deck(archidekt_id)
            if result and "error" not in result:
                flash_missing_cards(result)
                return flask.redirect(flask.url_for(deck_format))

            message = result.get("error") if isinstance(result, dict) else f"Failed to update deck URL: {form.url.data}"
            flask.flash(message)
        else:
            flask.flash(f"Bad archidekt URL: {form.url.data}")

        return flask.redirect(flask.url_for(deck_format))

    deck_data = list_decks(
        deck_format,
        experimental=experimental,
        owner_filter=request.args.get("owner"),
    )
    return flask.render_template(
        "index.html",
        title=deck_format,
        experimental=experimental,
        budget=budget,
        average_price=deck_data["average_price"],
        results=deck_data["decks"],
        form=form,
        owners=deck_data["owners"],
        update_form=UpdateForm(),
    )


@app.route("/", methods=["GET", "POST"])
def main():
    return render_deck_list("edh", budget=EDH_BUDGET)


@app.route("/beta", methods=["GET", "POST"])
def beta():
    return flask.render_template(
        "beta.html",
        title="edhbudget",
        form=SubmitForm(),
    )


@app.route("/faq", methods=["GET", "POST"])
def faq():
    return flask.render_template("faq.html", title="faq")


@app.route("/oathbreaker", methods=["GET", "POST"])
def oathbreaker():
    return render_deck_list("oathbreaker", budget=OATHBREAKER_BUDGET, experimental=True)


@app.route("/edh", methods=["GET", "POST"])
def edh():
    return render_deck_list("edh", budget=EDH_BUDGET)


@app.route("/edh-experimental", methods=["GET", "POST"])
def edh_experimental():
    return render_deck_list("edh", budget=EDH_BUDGET, experimental=True)


@app.route("/deck", methods=["GET", "POST"])
def deck():
    form = SubmitForm()
    if form.validate_on_submit():
        archidekt_id = parse_archidekt_id(form.url.data)
        result = update_deck(archidekt_id) if archidekt_id else None
        if result and "error" not in result:
            flash_missing_cards(result)
        else:
            message = result.get("error") if isinstance(result, dict) else f"Bad archidekt URL! {form.url.data}"
            flask.flash(message)
        return flask.redirect(flask.url_for("main"))

    deck_data = get_deck(request.args["archidekt_id"])
    if not deck_data:
        flask.abort(404)

    return flask.render_template(
        "deck.html",
        title="edhbudget",
        results=price_rows_for_template(deck_data),
        deck_name=deck_data["name"],
        archidekt_id=request.args["archidekt_id"],
        form=form,
        update_form=UpdateForm(),
    )


@app.route("/card_history")
def card_history():
    name = request.args.get("name")
    if not name:
        return flask.jsonify({"error": "missing name"}), 400

    return flask.jsonify({"name": name, "history": get_card_price_history(name)})


@app.route("/deck_history")
def deck_history():
    deck = get_deck(request.args.get("archidekt_id", ""))
    if not deck:
        return flask.jsonify({"error": "deck not found"}), 404

    names = [row[0] for row in price_rows_for_template(deck) if row[0] != "NEW CARD"]
    return flask.jsonify({
        "name": deck.get("name", "Deck"),
        "history": get_deck_price_history(names),
        "season": deck.get("deck_price_season"),
        "season_new": deck.get("deck_price_season_new"),
    })


@app.route("/robots.txt")
def robots_dot_txt():
    return "User-agent: *\nDisallow: /"


def flash_missing_cards(result):
    for card in result.get("missing_cards", []):
        flask.flash(f"Missing {card}, contact the admin")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=DEBUG)
