import unittest

from archidekt import deck_format_name, get_commander_in_archidekt, parse_archidekt_id


class ArchidektTests(unittest.TestCase):
    def test_parse_archidekt_id_accepts_full_url(self):
        self.assertEqual(parse_archidekt_id("https://archidekt.com/decks/123456/test-deck"), "123456")

    def test_parse_archidekt_id_accepts_bare_id(self):
        self.assertEqual(parse_archidekt_id("123456"), "123456")

    def test_parse_archidekt_id_rejects_url_without_id(self):
        self.assertIsNone(parse_archidekt_id("https://archidekt.com/decks/not-a-number"))

    def test_deck_format_name_maps_known_formats(self):
        self.assertEqual(deck_format_name(3), "edh")
        self.assertEqual(deck_format_name(14), "oathbreaker")
        self.assertEqual(deck_format_name(99), "other")

    def test_get_commander_in_archidekt_accepts_creatures(self):
        data = {
            "cards": [
                {
                    "categories": ["Commander"],
                    "card": {
                        "oracleCard": {
                            "name": "Example Commander",
                            "types": ["Legendary", "Creature"],
                        }
                    },
                }
            ]
        }

        self.assertEqual(get_commander_in_archidekt(data), "Example Commander")


if __name__ == "__main__":
    unittest.main()
