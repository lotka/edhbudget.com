import pandas as pd
import requests
from firebase_admin import firestore

from archidekt import edhrec_commander_url
from clients import db
from config import ARCHIDEKT_DECK_API, CARD_PRICE_COLLECTION, FIRESTORE_COLLECTION, TIMEZONE
from notifications import notify_new_deck
from pricing import calculate_price_archidekt, safe_doc_id


def get_card_price(card_name):
    doc_ref = db.collection(CARD_PRICE_COLLECTION).document(safe_doc_id(card_name))
    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict()

    return None


def update_deck(archidekt_id):
    url = ARCHIDEKT_DECK_API.format(deck_id=archidekt_id)
    deck_request = requests.get(url)
    if deck_request.status_code != 200:
        db.collection(FIRESTORE_COLLECTION).document(archidekt_id).delete()
        return False

    doc_ref = db.collection(FIRESTORE_COLLECTION).document(archidekt_id)
    is_new_deck = not doc_ref.get().exists
    result = calculate_price_archidekt(deck_request.json(), url, get_card_price)
    if "error" in result:
        return result

    doc_ref.set(result)
    if is_new_deck:
        notify_new_deck(result)

    return result


def format_modified_timestamp(value):
    return pd.to_datetime(value, utc=True).tz_convert(TIMEZONE).strftime("%Y-%m-%d %H:%M")


def prepare_deck_for_list(deck, deck_format, experimental):
    if deck["deckFormat"] != deck_format:
        return None

    if "[P]" not in deck["name"] and not experimental:
        return None

    deck = dict(deck)
    if not experimental:
        deck["name"] = deck["name"].replace("[P]", "")

    deck["modified"] = format_modified_timestamp(deck["modified"])
    deck.setdefault("deck_price_season", 0.00)
    deck.setdefault("deck_price_season_new", 0.00)
    deck["commander_url"] = edhrec_commander_url(deck["commander"], deck_format)
    return deck


def list_decks(deck_format, experimental=False, owner_filter=None):
    deck_ids_ref = db.collection(FIRESTORE_COLLECTION).order_by(
        "modified",
        direction=firestore.Query.DESCENDING,
    )

    decks = []
    owners = []
    for doc in deck_ids_ref.stream():
        deck = doc.to_dict()
        owners.append(deck["owner"])
        if owner_filter and deck["owner"] != owner_filter:
            continue

        prepared_deck = prepare_deck_for_list(deck, deck_format, experimental)
        if prepared_deck:
            decks.append(prepared_deck)

    average_price = 0
    if decks:
        average_price = sum(deck["deck_price_season"] for deck in decks) / float(len(decks))

    return {
        "decks": decks,
        "owners": sorted(set(owners)),
        "average_price": round(average_price, 2),
    }


def get_deck(archidekt_id):
    doc_ref = db.collection(FIRESTORE_COLLECTION).document(archidekt_id)
    doc = doc_ref.get()
    if not doc.exists:
        return None

    return doc.to_dict()


def price_rows_for_template(deck):
    price_list = deck.get("price_list", [])
    if not price_list:
        return []

    if isinstance(price_list[0], dict):
        return [
            [row["name"], row["price_season"], row["price_season_new"]]
            for row in price_list
        ]

    return [
        [price_list[i], price_list[i + 1], price_list[i + 2]]
        for i in range(0, len(price_list), 3)
    ]
