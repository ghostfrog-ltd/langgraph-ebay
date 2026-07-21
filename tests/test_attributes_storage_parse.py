import unittest

from pipelines.listing.attributes.graph import _parse_storage_gb


class AttributeStorageParseTests(unittest.TestCase):
    def test_parses_gb_storage(self):
        self.assertEqual(_parse_storage_gb("256 GB"), 256)
        self.assertEqual(_parse_storage_gb("64GB"), 64)

    def test_converts_tb_storage(self):
        self.assertEqual(_parse_storage_gb("1 TB"), 1024)
        self.assertEqual(_parse_storage_gb("1.5TB"), 1536)

    def test_uses_largest_unit_bearing_value_for_mixed_ram_storage(self):
        self.assertEqual(_parse_storage_gb("8GB RAM / 256GB Storage"), 256)

    def test_rejects_catalogue_codes_and_impossible_values(self):
        self.assertIsNone(_parse_storage_gb("A1507 202605301234567890"))
        self.assertIsNone(_parse_storage_gb("999999999999 GB"))

    def test_accepts_short_numeric_legacy_values_only(self):
        self.assertEqual(_parse_storage_gb("512"), 512)
        self.assertIsNone(_parse_storage_gb("1234567890"))


if __name__ == "__main__":
    unittest.main()
