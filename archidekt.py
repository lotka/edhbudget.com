def get_cards_in_archidekt(data):
    cards = []
    for card in data['cards']:
        if 'Basic' not in card['card']['oracleCard']['superTypes'] and 'Maybeboard' not in card['categories']:
            cards.append(card['card']['oracleCard']['name'])
    return set(cards)

def get_commander_in_archidekt(data):
    commander = None
    for card in data['cards']:
        if 'Commander' in card['categories'] and 'Planeswalker' in card['card']['oracleCard']['types']:
            commander = card['card']['oracleCard']['name']
    return commander