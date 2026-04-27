from urllib.parse import urlparse


DECK_FORMATS = {
    3: "edh",
    14: "oathbreaker",
}


def parse_archidekt_id(url):
    if url.isdigit():
        return url

    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    for part in parts:
        if part.isdigit():
            return part

    return None


def deck_format_name(deck_format_id):
    return DECK_FORMATS.get(deck_format_id, "other")


def card_name(card_entry):
    return card_entry["card"]["oracleCard"]["name"]


def oracle_card(card_entry):
    return card_entry["card"]["oracleCard"]


def get_commander_in_archidekt(data):
    for card in data["cards"]:
        categories = card.get("categories")
        if categories is None or "Commander" not in categories:
            continue

        types = oracle_card(card).get("types", [])
        if "Planeswalker" in types or "Creature" in types:
            return card_name(card)

    return "Commander not found"


def is_priced_deck_card(card_entry):
    categories = card_entry.get("categories")
    if categories is None:
        raise ValueError(f"Card {card_name(card_entry)} has no categories, failed to add deck")

    card = oracle_card(card_entry)
    return (
        "Basic" not in card.get("superTypes", [])
        and "Maybeboard" not in categories
        and "Sideboard" not in categories
    )


def get_cards_in_archidekt(data):
    return {
        card_name(card)
        for card in data["cards"]
        if is_priced_deck_card(card)
    }


def edhrec_commander_url(commander, deck_format):
    slug = commander.replace(",", "").replace(" ", "-").lower()
    if deck_format == "oathbreaker":
        return f"https://oathbreaker.edhrec.com/oathbreakers/{slug}"

    return f"https://edhrec.com/commanders/{slug}"
