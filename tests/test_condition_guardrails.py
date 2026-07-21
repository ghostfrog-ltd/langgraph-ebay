import unittest

from utils.condition import looks_like_partial_or_accessory_listing


class ConditionGuardrailTests(unittest.TestCase):
    def test_mini_number_plate_holder_is_vehicle_accessory(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "2 x 3D Gloss Gel Mini Cooper S Quick Release - Number Plate Holder Surround",
                source="ebay-mini-cooper",
                model_key="mini-cooper_b",
            )
        )

    def test_mini_project_car_is_not_accessory(self):
        self.assertFalse(
            looks_like_partial_or_accessory_listing(
                "Mini Cooper project",
                source="ebay-mini-cooper",
                model_key="mini-cooper_b",
            )
        )

    def test_console_controller_mount_is_accessory(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Controller Wall Mount Storage Bracket Earphone Holder For PS5/PS4/XBox/Switch",
                source="ebay-consoles",
                model_key="ps5_b",
            )
        )

    def test_playstation_console_is_not_accessory(self):
        self.assertFalse(
            looks_like_partial_or_accessory_listing(
                "Sony PS5 Blu Ray Disk Edition 825GB With Controller and Leads",
                source="ebay-consoles",
                model_key="ps5_825gb_b",
            )
        )

    def test_console_headset_is_accessory(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Gaming Headset With Microphone Headphones Mic 3.5mm For PC Laptop Xbox One PS4/5",
                source="ebay-consoles",
                model_key="ps4_b",
            )
        )

    def test_xbox_power_supply_is_accessory(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Genuine Microsoft Xbox One PSU Power Supply A13-203P1A official brick",
                source="ebay-consoles",
                model_key="xbox_one_b",
            )
        )

    def test_switch_code_in_a_box_is_not_console_hardware(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Capcom Arcade Stadium 1+2 (Nintendo Switch Code In A Box)",
                source="ebay-consoles",
                model_key="switch_b",
            )
        )

    def test_xbox_button_led_stickers_are_accessory(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Extremerate Custom Home Guide Button LED Mod Stickers for Xbox Series X/S",
                source="ebay-consoles",
                model_key="xbox_series_b",
            )
        )

    def test_macbook_needs_new_battery_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple Macbook Air 2017 13 inch 8gb RAM / 128 SSD NEEDS NEW BATTERY",
                source="ebay-apple",
                model_key="apple-macbook-air-13in-intel-8gb-256gb_b",
            )
        )

    def test_locked_iphone_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Locked Apple iPhone 11 64GB, Black",
                source="ebay-apple",
                model_key="apple-iphone-11-64gb_b",
            )
        )

    def test_apple_spares_slash_repair_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple MacBook Pro M1 8GB 256GB SSD - Spares / Repair",
                source="ebay-apple",
                model_key="apple-macbook-pro-13in-m1-8gb-256gb_b",
            )
        )

    def test_iphone_works_with_issues_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "APPLE IPHONE 7 - 32GB - UNLOCKED CHEAP SMARTPHONE - WORKS WITH ISSUES",
                source="ebay-apple",
                model_key="apple-iphone-7-32gb_b",
            )
        )

    def test_ps5_game_is_not_console_hardware(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Like a Dragon: Pirate Yakuza in Hawaii (Playstation 5 PS5) - NEW & SEALED",
                source="ebay-consoles",
                model_key="ps5_a",
            )
        )

    def test_switch_game_title_is_not_console_hardware(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Pokemon Legends Z-A Nintendo Switch",
                source="ebay-consoles",
                model_key="switch_b",
            )
        )


if __name__ == "__main__":
    unittest.main()
