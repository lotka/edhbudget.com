import logging
from functools import lru_cache

import requests
from google.cloud import secretmanager

from config import DISCORD_WEBHOOK_SECRET_ID, PROJECT_ID


@lru_cache(maxsize=1)
def get_discord_webhook_url():
    secret_name = f"projects/{PROJECT_ID}/secrets/{DISCORD_WEBHOOK_SECRET_ID}/versions/latest"
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8").strip()


def notify_new_deck(result):
    try:
        webhook_url = get_discord_webhook_url()
    except Exception:
        logging.exception("Failed to load Discord webhook secret")
        return

    if not webhook_url:
        return

    message = (
        f"New deck added: **{result['name']}** by **{result['owner']}**\n"
        f"{result['url']}\n"
        f"Format: `{result['deckFormat']}` | Price: `${result['deck_price_season']}`"
    )

    try:
        requests.post(webhook_url, json={"content": message}, timeout=10)
    except Exception:
        logging.exception("Failed to send Discord webhook notification")
