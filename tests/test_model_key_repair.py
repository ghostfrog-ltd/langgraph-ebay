import unittest

from pipelines.listing.attributes.mk import normalise_model


class ModelKeyRepairTests(unittest.TestCase):
    def test_apple_normalised_attrs_are_aliased(self):
        key = normalise_model(
            title="iPhone 11 64GB White Unlocked",
            attrs={"brand": "Apple", "model_name": "Apple iPhone 11", "storage_gb": 64},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-iphone-11-64gb_b")

    def test_console_game_does_not_become_console_key(self):
        key = normalise_model(
            title="L.A Noire Game for Sony PS4 Playstation 4",
            attrs={"Platform": "Sony PlayStation 4", "Game Name": "L.A Noire"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_console_hardware_from_normalised_attrs(self):
        key = normalise_model(
            title="Xbox Series X 1tb, Controller Is Power A Wired, No Box",
            attrs={"brand": "Microsoft", "model_name": "Microsoft Xbox Series X"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "xbox_series_1tb_b")

    def test_switch_tablet_only_gets_separate_key(self):
        key = normalise_model(
            title="Nintendo Switch Console/tablet Only",
            attrs={"Platform": "Nintendo Switch"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "switch_tablet_b")

    def test_ps4_slim_storage_gets_separate_key(self):
        key = normalise_model(
            title="Sony PlayStation 4 PS4 Slim 500GB Console - Black",
            attrs={"Platform": "Sony PlayStation 4"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "ps4_slim_500gb_b")

    def test_ps5_game_title_does_not_become_console_key(self):
        key = normalise_model(
            title="Like a Dragon: Pirate Yakuza in Hawaii (Playstation 5 PS5) - NEW & SEALED",
            attrs={"Platform": "Sony PlayStation 5"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_switch_game_title_does_not_become_console_key(self):
        key = normalise_model(
            title="Pokemon Legends Z-A Nintendo Switch",
            attrs={"Platform": "Nintendo Switch"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_storage_signal_keeps_ps5_hardware(self):
        key = normalise_model(
            title="Sony PS5 Blu Ray Disk Edition 825GB With Controller and Leads",
            attrs={"Platform": "Sony PlayStation 5"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "ps5_825gb_b")

    def test_console_replacement_part_does_not_become_console_key(self):
        key = normalise_model(
            title="Sony PlayStation 5 PS5 HDMI Port Replacement Repair Part",
            attrs={"Platform": "Sony PlayStation 5"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_playstation_portal_does_not_become_ps5_key(self):
        key = normalise_model(
            title="Sony PlayStation Portal Remote Player for PS5 Fully Boxed",
            attrs={"Platform": "Sony PlayStation 5"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_lego_set_number_is_used(self):
        key = normalise_model(
            title="LEGO Creator 3in1 Cute Hamster with a Flower Toy",
            attrs={"Brand": "LEGO", "LEGO Theme": "Creator", "LEGO Set Number": "31376"},
            source="ebay-lego",
        )

        self.assertEqual(key, "lego-31376_b-c")

    def test_lego_short_piece_count_is_not_used_as_set_number(self):
        key = normalise_model(
            title="LEGO Creator 3in1 Cute Hamster with a Flower Toy",
            attrs={"Brand": "LEGO", "LEGO Theme": "Creator", "LEGO Set Number": "166"},
            source="ebay-lego",
        )

        self.assertEqual(key, "unknown")

    def test_lego_theme_and_retired_word_do_not_fragment_set_number(self):
        key = normalise_model(
            title="LEGO Ideas A-Frame Cabin 21338 retired",
            attrs={},
            source="ebay-lego",
        )

        self.assertEqual(key, "lego-21338_b-c")

    def test_bike_capacity_digits_are_not_trimmed_as_year(self):
        key = normalise_model(
            title="Lot#66848 2015 YAMAHA YZF-R125 ABS -SPARES/REPAIR",
            attrs={
                "Manufacturer": "YAMAHA",
                "Model": "YZF R125 ABS",
                "Capacity (cc)": "124",
            },
            source="motomine",
        )

        self.assertEqual(key, "yamaha-yzfr125-125cc_b")

    def test_tools_title_only_mafell_model_key(self):
        key = normalise_model(
            title="Mafell A 12 Cordless 12V Impact Drill Driver 2 x Batteries Charger T-Max Case",
            attrs={},
            source="ebay-tools",
        )

        self.assertEqual(key, "mafell-a12-12v-kit_b")

    def test_tools_title_model_beats_catalogue_code(self):
        key = normalise_model(
            title="Mafell A 12 Cordless 12V Impact Drill Driver 2 x Batteries Charger T-Max Case",
            attrs={
                "Brand": "MAFELL",
                "Model": "91D021",
                "MPN": "91D021",
                "Voltage": "12 V",
                "Motor Type": "Brushless",
            },
            source="ebay-tools",
        )

        self.assertEqual(key, "mafell-a12-12v-brushless-kit_b")

    def test_actioncam_title_only_gopro_hero_model_key(self):
        key = normalise_model(
            title="GoPro HERO12 Black Action Camera Accessory Bundle CHDRB121-A",
            attrs={},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "gopro-hero12_b")

    def test_actioncam_structured_model_drops_brand_prefix(self):
        key = normalise_model(
            title="GoPro HERO12 Black Action Camera Accessory Bundle CHDRB121-A",
            attrs={"Brand": "GoPro", "Model": "GoPro HERO12", "Type": "Helmet/Action"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "gopro-hero12_b")

    def test_actioncam_title_beats_wrong_copied_model_attribute(self):
        key = normalise_model(
            title="GoPro HERO12 Black Action Camera & 3 Batteries & Charger",
            attrs={"Brand": "GoPro", "Model": "GoPro Hero 3+", "Type": "Helmet/Action"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "gopro-hero12_b")

    def test_actioncam_lens_mod_does_not_become_camera_key(self):
        key = normalise_model(
            title="Genuine GoPro Max Lens Mod 2.0 for HERO 12",
            attrs={},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "unknown")

    def test_watch_title_only_reference_and_kinetic_model_key(self):
        key = normalise_model(
            title="Seiko Titanium Flightmaster GMT AGS Kinetic 5M45-6A50, 90s Classic",
            attrs={},
            source="ebay-watches",
        )

        self.assertEqual(key, "seiko-5m456a50-kinetic-watchonly_b")

    def test_watch_without_paper_signal_is_watch_only(self):
        key = normalise_model(
            title="Seiko 5 Automatic Watch",
            attrs={},
            source="ebay-watches",
        )

        self.assertEqual(key, "seiko-seiko5-auto-watchonly_b")


if __name__ == "__main__":
    unittest.main()
