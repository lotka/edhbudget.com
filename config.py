import os
import sys


DEBUG = len(sys.argv) > 1 and sys.argv[1] == "dev"

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "nifty-beast-realm"
FIRESTORE_COLLECTION = "deck-ids-dev" if DEBUG else "deck-ids"
CARD_PRICE_COLLECTION = "card-prices-v2"
PRICE_TABLE = "nifty-beast-realm.magic.scryfall-prices-v2"
DISCORD_WEBHOOK_SECRET_ID = "discord-webhook-url"
SECRET_KEY = os.getenv("SECRET_KEY", "you-will-never-guess")
TIMEZONE = "Europe/London"

ARCHIDEKT_DECK_API = "https://archidekt.com/api/decks/{deck_id}/"
EDH_BUDGET = 60
OATHBREAKER_BUDGET = 35
