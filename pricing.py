import hashlib

import pandas as pd

from archidekt import card_name, deck_format_name, get_commander_in_archidekt, is_priced_deck_card


PRICE_FIELDS = ["name", "price_season", "price_season_new", "price_season_combined"]


def safe_doc_id(name: str) -> str:
    return hashlib.md5(name.encode()).hexdigest()


def display_value(value):
    if pd.isnull(value):
        return "NEW CARD"

    return value


def build_price_rows(historical):
    price_list = historical[PRICE_FIELDS].sort_values(by="price_season_combined", ascending=False)
    price_list["price_season"] = price_list["price_season"].astype(float).round(2)
    price_list["price_season_new"] = price_list["price_season_new"].round(2)
    price_list = price_list.round(2)

    return [
        {
            "name": display_value(row["name"]),
            "price_season": display_value(row["price_season"]),
            "price_season_new": display_value(row["price_season_new"]),
        }
        for _, row in price_list.iterrows()
    ]


def calculate_price_archidekt(data, url, get_card_price):
    commander = get_commander_in_archidekt(data)
    deck_format = deck_format_name(data["deckFormat"])
    cards = []

    for card in data["cards"]:
        try:
            if is_priced_deck_card(card):
                cards.append(card_name(card))
        except ValueError as exc:
            return {"error": str(exc)}

    if not cards:
        return {
            "name": data["name"],
            "owner": data["owner"]["username"],
            "url": url.replace("/api", ""),
            "commander": commander,
            "commander_url": None,
            "cards": 0,
            "commander_price": 0,
            "free_cards": 0,
            "id": data["id"],
            "modified": str(data["updatedAt"]),
            "price_list": [],
            "deckFormat": "n/a",
            "deck_price_season": 0.0000000000000001,
            "deck_price_season_new": 0.0000000000000001,
            "deck_price": 0.0000000000000001,
            "deck_price_change": 0,
            "missing_cards": [],
        }

    card_prices = []
    missing_cards = []
    for card in cards:
        price = get_card_price(card)
        if price:
            card_prices.append(price)
        else:
            missing_cards.append(card)

    historical = pd.DataFrame(card_prices)
    if historical.empty:
        return {"error": "No card prices found for deck"}

    price_rows = build_price_rows(historical)
    deck_price = round(historical["price_season_combined"].sum(), 2)

    return {
        "name": data["name"],
        "owner": data["owner"]["username"],
        "url": url.replace("/api", ""),
        "commander": commander,
        "cards": len(historical),
        "commander_price": round(historical["price_season_combined"][historical.name == commander].sum(), 2),
        "free_cards": int((historical["price_season_combined"] == 0).sum()),
        "id": data["id"],
        "modified": str(data["updatedAt"]),
        "price_list": price_rows,
        "deckFormat": deck_format,
        "deck_price_season": deck_price,
        "deck_price_season_new": round(historical["price_season_new"].sum(), 2),
        "deck_price": deck_price,
        "deck_price_change": 0,
        "missing_cards": missing_cards,
    }
