import unittest

try:
    from pricing import calculate_price_archidekt
except ModuleNotFoundError as exc:
    if exc.name != "pandas":
        raise
    calculate_price_archidekt = None


@unittest.skipIf(calculate_price_archidekt is None, "pandas is not installed in this Python environment")
class PricingTests(unittest.TestCase):
    def test_calculate_price_archidekt_builds_structured_price_rows(self):
        deck_data = {
            "name": "Example Deck",
            "owner": {"username": "alice"},
            "deckFormat": 3,
            "id": 123,
            "updatedAt": "2026-04-27T12:00:00Z",
            "cards": [
                {
                    "categories": ["Commander"],
                    "card": {
                        "oracleCard": {
                            "name": "Example Commander",
                            "types": ["Creature"],
                            "superTypes": ["Legendary"],
                        }
                    },
                },
                {
                    "categories": ["Mainboard"],
                    "card": {
                        "oracleCard": {
                            "name": "Example Spell",
                            "types": ["Sorcery"],
                            "superTypes": [],
                        }
                    },
                },
                {
                    "categories": ["Mainboard"],
                    "card": {
                        "oracleCard": {
                            "name": "Forest",
                            "types": ["Basic", "Land"],
                            "superTypes": ["Basic"],
                        }
                    },
                },
            ],
        }
        prices = {
            "Example Commander": {
                "name": "Example Commander",
                "price_season": 1.234,
                "price_season_new": 2.345,
                "price_season_combined": 1.234,
            },
            "Example Spell": {
                "name": "Example Spell",
                "price_season": 3.456,
                "price_season_new": 4.567,
                "price_season_combined": 3.456,
            },
        }

        result = calculate_price_archidekt(
            deck_data,
            "https://archidekt.com/api/decks/123/",
            prices.get,
        )

        self.assertEqual(result["deckFormat"], "edh")
        self.assertEqual(result["cards"], 2)
        self.assertEqual(result["commander"], "Example Commander")
        self.assertEqual(result["deck_price_season"], 4.69)
        self.assertEqual(result["price_list"][0]["name"], "Example Spell")
        self.assertEqual(result["price_list"][0]["price_season"], 3.46)


if __name__ == "__main__":
    unittest.main()
