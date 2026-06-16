from datetime import date
from functools import lru_cache

from google.cloud import bigquery

from config import PRICE_TABLE, PROJECT_ID


_client = bigquery.Client(project=PROJECT_ID)

HISTORY_QUERY = f"""
SELECT DATE(datetime) AS day, MIN(main_price_usd) AS usd
FROM `{PRICE_TABLE}`
WHERE name = @name AND main_price_usd IS NOT NULL
GROUP BY day
ORDER BY day
"""


DECK_HISTORY_QUERY = f"""
SELECT day, SUM(usd) AS usd FROM (
    SELECT name, DATE(datetime) AS day, MIN(main_price_usd) AS usd
    FROM `{PRICE_TABLE}`
    WHERE name IN UNNEST(@names) AND main_price_usd IS NOT NULL
    GROUP BY name, day
)
GROUP BY day
ORDER BY day
"""


def _rows_to_series(job):
    return [
        {"day": row["day"].isoformat(), "usd": float(row["usd"])}
        for row in job.result()
    ]


@lru_cache(maxsize=2048)
def _query_history(name, _cache_day):
    # _cache_day is part of the cache key so results refresh once per day,
    # matching the daily cadence of the price pipeline.
    job = _client.query(
        HISTORY_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", name)],
        ),
    )
    return _rows_to_series(job)


@lru_cache(maxsize=512)
def _query_deck_history(names, _cache_day):
    job = _client.query(
        DECK_HISTORY_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("names", "STRING", list(names))],
        ),
    )
    return _rows_to_series(job)


def get_card_price_history(name):
    return _query_history(name, date.today())


def get_deck_price_history(names):
    # sorted+deduped so the cache key is stable regardless of card order.
    return _query_deck_history(tuple(sorted(set(names))), date.today())
