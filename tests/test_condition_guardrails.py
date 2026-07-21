import unittest

from utils.condition import has_investible_product_identity, looks_like_partial_or_accessory_listing


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

    def test_ipad_motherboard_is_device_part(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple iPad 10th Gen A2757 10.9 inch 64GB, Wi-Fi+Cellular, Motherboard (112)",
                source="ebay-apple",
                model_key="apple-ipad-10th-gen-64gb_b",
            )
        )

    def test_iphone_logic_board_is_device_part(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple iPhone 13 Pro 128GB Logic Board Only",
                source="ebay-apple",
                model_key="apple-iphone-13-pro-128gb_b",
            )
        )

    def test_ipad_screen_assembly_is_device_part(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple iPad Pro 11 inch LCD Screen Assembly Replacement",
                source="ebay-apple",
                model_key="apple-ipad-pro-11in_b",
            )
        )

    def test_iphone_back_housing_is_device_part(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple iPhone 12 Blue Back Housing Chassis Frame",
                source="ebay-apple",
                model_key="apple-iphone-12-64gb_b",
            )
        )

    def test_macbook_display_assembly_is_device_part(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "MacBook Pro 13 inch A2338 Display Assembly Space Grey",
                source="ebay-apple",
                model_key="apple-macbook-pro-13in-m1-8gb-256gb_b",
            )
        )

    def test_macbook_parts_missing_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple Intel Macbook A1181 White Laptop - Parts Missing",
                source="ebay-apple",
                model_key="apple-macbook-intel_b",
            )
        )

    def test_macbook_broken_screen_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple MacBook Pro A2442 M1 Pro Working, Broken LCD Screen",
                source="ebay-apple",
                model_key="apple-macbook-pro-m1pro-16gb-1024gb_b",
            )
        )

    def test_dead_macbook_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple MacBook Air A1466 DEAD, specs unknown",
                source="ebay-apple",
                model_key="apple-macbook-air-133in-intel-256gb_b",
            )
        )

    def test_macbook_neo_is_identity_pollution(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                'NEW SEALED Apple MacBook Neo 13" A18 Pro 8GB 512GB SSD Touch ID',
                source="ebay-apple",
                model_key="apple-macbook-pro-13in-8gb-512gb_a",
            )
        )

    def test_macbook_with_a_series_phone_chip_is_identity_pollution(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                'Apple MacBook 13-inch Laptop with A18 Pro chip',
                source="ebay-apple",
                model_key="apple-macbook-13in-8gb-256gb_b",
            )
        )

    def test_real_macbook_a_model_number_is_not_identity_pollution(self):
        self.assertFalse(
            looks_like_partial_or_accessory_listing(
                'Apple MacBook A1534 12-inch 2017 Intel Core M3 256GB SSD 8GB RAM',
                source="ebay-apple",
                model_key="apple-macbook-12in-m3-8gb-256gb_b",
            )
        )

    def test_apple_joblot_is_not_single_device_comp(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "JOBLOT Apple iPhone SE Working",
                source="ebay-apple",
                model_key="apple-iphone-32gb_b",
            )
        )

    def test_unit_only_macbook_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "APPLE MACBOOK AIR M2, 2022 1TB LAPTOP IN SPACE GREY: MODEL NO A2681 (UNIT ONLY).",
                source="ebay-apple",
                model_key="apple-macbook-air-m2-1024gb_b",
            )
        )

    def test_mdm_locked_iphone_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple iPhone 12 64GB Black MDM Locked Remote Management",
                source="ebay-apple",
                model_key="apple-iphone-12-64gb_b",
            )
        )

    def test_good_iphone_is_not_accessory(self):
        self.assertFalse(
            looks_like_partial_or_accessory_listing(
                "Apple iPhone XR 64GB Unlocked Black Good Condition",
                source="ebay-apple",
                model_key="apple-iphone-xr-64gb_b",
            )
        )

    def test_broad_iphone_storage_key_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "Apple iPhone 128GB Unlocked",
                source="ebay-apple",
                model_key="apple-iphone-128gb_b",
            )
        )

    def test_unknown_ipad_generation_key_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "Apple iPad 11-inch Wi-Fi 128GB",
                source="ebay-apple",
                model_key="apple-ipad-11in-gen-unk-128gb_b",
            )
        )

    def test_clean_m_series_macbook_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "Apple MacBook Air M1 13 inch 8GB 256GB Good Condition",
                source="ebay-apple",
                model_key="apple-macbook-air-13in-m1-8gb-256gb_b",
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
