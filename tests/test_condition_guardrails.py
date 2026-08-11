import unittest

from utils.condition import (
    _derive_condition_grade,
    has_investible_product_identity,
    looks_like_partial_or_accessory_listing,
)


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

    def test_switch_joy_con_is_accessory_not_console(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Compatible Joy-Con 2 for Nintendo Switch 2 - Purple/Green - Open Box",
                source="ebay-consoles",
                model_key="nintendo-switch-2",
            )
        )

    def test_switch_usb_c_cable_attrs_do_not_make_it_console(self):
        attrs = {
            "Type": "USB Cable",
            "Compatible Product": "Console",
            "Platform": "Nintendo Switch",
        }

        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Nintendo Switch Official USB-C Cable (USB-A to USB-C)",
                attrs,
                source="ebay-consoles",
                model_key="nintendo-switch-base",
            )
        )
        self.assertFalse(
            has_investible_product_identity(
                "Nintendo Switch Official USB-C Cable (USB-A to USB-C)",
                attrs,
                source="ebay-consoles",
                model_key="nintendo-switch-base",
            )
        )

    def test_switch_carrying_case_attrs_do_not_make_it_console(self):
        attrs = {
            "Type": "Carrying Cases",
            "Compatible Product": ["Console", "Game(s)", "Game Card"],
            "Platform": "Nintendo Switch",
        }

        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Super Mario Nintendo Switch Lite Power A Stealth Case",
                attrs,
                source="ebay-consoles",
                model_key="nintendo-switch-lite",
            )
        )
        self.assertFalse(
            has_investible_product_identity(
                "Super Mario Nintendo Switch Lite Power A Stealth Case",
                attrs,
                source="ebay-consoles",
                model_key="nintendo-switch-lite",
            )
        )

    def test_switch_replacement_battery_is_not_investible_console(self):
        title = "BATTERY FOR NINTENDO SWITCH CONSOLE HAC-003 REPLACEMENT - NEW"
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                title,
                source="ebay-consoles",
                model_key="nintendo-switch-base",
            )
        )
        self.assertFalse(
            has_investible_product_identity(
                title,
                source="ebay-consoles",
                model_key="nintendo-switch-base",
            )
        )

    def test_playstation_console_is_not_accessory(self):
        self.assertFalse(
            looks_like_partial_or_accessory_listing(
                "Sony PS5 Blu Ray Disk Edition 825GB With Controller and Leads",
                source="ebay-consoles",
                model_key="sony-playstation-5-disc-825gb",
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

    def test_unlocked_iphone_is_not_blocked_by_locked_guard(self):
        self.assertFalse(
            looks_like_partial_or_accessory_listing(
                "Apple iPhone 13 128GB Midnight Black Unlocked Smartphone",
                source="ebay-apple",
                model_key="apple-iphone-13-128gb",
            )
        )

    def test_locked_macbook_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                'Apple MacBook Pro (16-inch, 2019) - i7, 16GB RAM, 500GB SSD - A2141 - LOCKED',
                source="ebay-apple",
                model_key="apple-macbook-pro-a2141-16in-core-i7-16gb-512gb",
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

    def test_macbook_keyboard_palmrest_touchpad_battery_is_device_part(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple MacBook Pro A2141 UK Laptop Keyboard Palmrest Touchpad Battery Assembly",
                source="ebay-apple",
                model_key="apple-macbook-pro-a2141-16in-core-i7-16gb-512gb",
            )
        )
        self.assertFalse(
            has_investible_product_identity(
                "Apple MacBook Pro A2141 UK Laptop Keyboard Palmrest Touchpad Battery Assembly",
                source="ebay-apple",
                model_key="apple-macbook-pro-a2141-16in-core-i7-16gb-512gb",
            )
        )

    def test_macbook_faulty_scrap_note_in_attrs_is_not_investible(self):
        title = "Apple MacBook Pro 13 A2338 2020 M1 Space Grey"
        attrs = {
            "Seller notes": (
                "The macbook is faulty and I am selling for scraps. "
                "The screen works but it's badly damaged. "
                "The Logic board and touch ID is missing. "
                "No charger or accessories included. "
                "This is listed for faulty and not working."
            )
        }

        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                title,
                attrs,
                source="ebay-apple",
                model_key="apple-macbook-pro-a2338-13in-m1-8gb-256gb",
            )
        )
        self.assertFalse(
            has_investible_product_identity(
                title,
                attrs,
                source="ebay-apple",
                model_key="apple-macbook-pro-a2338-13in-m1-8gb-256gb",
            )
        )

    def test_macbook_screen_is_damaged_note_is_not_grade_b(self):
        title = "Apple MacBook Air 13in (256GB SSD, M1, 8GB) Laptop - Space Grey"
        attrs = {
            "Seller notes": (
                "Working 2020 M1 Macbook Air, left side of the screen is damaged "
                "but the laptop is still useable. Battery life 91%. All ports working, "
                "minor cosmetic damage to outside of laptop."
            )
        }

        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                title,
                attrs,
                source="ebay-apple",
                model_key="apple-macbook-air-13in-m1-8gb-256gb",
            )
        )
        self.assertFalse(
            has_investible_product_identity(
                title,
                attrs,
                source="ebay-apple",
                model_key="apple-macbook-air-13in-m1-8gb-256gb",
            )
        )
        self.assertEqual(_derive_condition_grade(attrs, title), "C")

    def test_working_iphone_with_visible_damage_is_condition_c(self):
        title = "iPhone 15 pro max. 256gb"
        attrs = {
            "Seller notes": (
                "Used on Vodafone but believed to be unlocked now phone is restored "
                "to factory settings. As per pictures there is damage to phone "
                "which does not affect usage and is fully working. Battery health "
                "at 86%. Comes with original box."
            )
        }

        self.assertEqual(_derive_condition_grade(attrs, title), "C")

    def test_physical_damage_caps_any_listing_at_condition_c(self):
        examples = [
            (
                "Sony PS5 Disc Edition 825GB Console",
                {"Seller notes": "Cracked casing but tested and fully working."},
            ),
            (
                "GoPro HERO12 Black Action Camera",
                {"Seller notes": "Smashed rear glass, camera is otherwise fully working."},
            ),
            (
                "BMW 1 Series 2014",
                {"Seller notes": "Dent in rear panel, starts and drives."},
            ),
        ]

        for title, attrs in examples:
            with self.subTest(title=title):
                self.assertEqual(_derive_condition_grade(attrs, title), "C")

    def test_fully_working_with_cosmetic_marks_stays_condition_b(self):
        self.assertEqual(
            _derive_condition_grade(
                {"Seller notes": "Fully working with cosmetic marks from normal use."},
                "Sony PS5 Disc Edition 825GB Console",
            ),
            "B",
        )

    def test_functional_fault_or_risk_is_condition_d_for_any_listing(self):
        examples = [
            (
                "iPhone 15 Pro Max 256GB",
                {"Seller notes": "Fully working except Face ID does not work."},
            ),
            (
                "Sony PS5 Disc Edition 825GB Console",
                {"Seller notes": "Works except HDMI fault."},
            ),
            (
                "Xbox Wireless Controller",
                {"Seller notes": "Tested but has stick drift."},
            ),
            (
                "GoPro HERO12 Black Action Camera",
                {"Seller notes": "Untested from house clearance."},
            ),
        ]

        for title, attrs in examples:
            with self.subTest(title=title):
                self.assertEqual(_derive_condition_grade(attrs, title), "D")

    def test_contradictory_functional_wording_requires_manual_review(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Sony PS5 Disc Edition 825GB Console",
                {"Seller notes": "Fully working but has an intermittent HDMI fault."},
                source="ebay-consoles",
                model_key="sony-playstation-5-disc-825gb",
            )
        )

    def test_damaged_rtx_5090_is_not_investible_normal_stock(self):
        title = "*DAMAGED* NVIDIA GeForce RTX 5090 32GB Graphics Card"

        self.assertEqual(_derive_condition_grade({}, title), "C")
        self.assertFalse(
            has_investible_product_identity(
                title,
                source="ebay-graphics-cards",
                model_key="nvidia-rtx-5090-32gb",
            )
        )

    def test_damaged_macbook_is_not_investible_normal_stock(self):
        title = "*DAMAGED* Apple MacBook Pro 14 inch M3 Pro 18GB 512GB"

        self.assertEqual(_derive_condition_grade({}, title), "C")
        self.assertFalse(
            has_investible_product_identity(
                title,
                source="ebay-apple",
                model_key="apple-macbook-pro-14in-m3pro-18gb-512gb",
            )
        )

    def test_clean_damage_negation_does_not_trigger_damage_guard(self):
        self.assertEqual(
            _derive_condition_grade(
                {"Seller notes": "Fully working with no damage."},
                "Sony PS5 Disc Edition 825GB Console",
            ),
            "B",
        )

    def test_screen_damage_variants_and_typos_are_not_investible(self):
        examples = [
            "Nintendo Switch 32GB Home Console - Black *SCRATHED SCREEN*",
            "Nintendo Switch 32GB Home Console - Black scratched-screen",
            "Nintendo Switch 32GB Home Console - Black scratch screen",
            "Nintendo Switch 32GB Home Console - Black screen-damaged",
            "Nintendo Switch 32GB Home Console - Black DAMAGED!!",
            "Nintendo Switch 32GB Home Console - Black faulty-screen",
        ]

        for title in examples:
            with self.subTest(title=title):
                self.assertEqual(_derive_condition_grade({}, title), "C" if "faulty" not in title.lower() else "D")
                self.assertFalse(
                    has_investible_product_identity(
                        title,
                        source="ebay-consoles",
                        model_key="nintendo-switch-base",
                    )
                )

    def test_negated_scratch_wording_does_not_trigger_damage_guard(self):
        self.assertEqual(
            _derive_condition_grade(
                {"Seller notes": "Fully working with no scratches or cracks."},
                "Nintendo Switch 32GB Home Console - Black",
            ),
            "B",
        )

    def test_apple_unlocked_language_is_not_lock_risk(self):
        for title in (
            "Unlocked Apple iPhone",
            "Network Unlocked Apple iPhone",
            "Factory Unlocked Apple iPhone",
        ):
            with self.subTest(title=title):
                self.assertFalse(
                    looks_like_partial_or_accessory_listing(
                        title,
                        source="ebay-apple",
                        model_key="apple-iphone-14-pro-128gb",
                    )
                )
                self.assertTrue(
                    has_investible_product_identity(
                        title,
                        source="ebay-apple",
                        model_key="apple-iphone-14-pro-128gb",
                    )
                )

    def test_apple_locked_language_is_lock_risk(self):
        for title in (
            "Locked Apple iPhone",
            "Network Locked Apple iPhone",
            "Activation Locked Apple iPhone",
        ):
            with self.subTest(title=title):
                self.assertTrue(
                    looks_like_partial_or_accessory_listing(
                        title,
                        source="ebay-apple",
                        model_key="apple-iphone-14-pro-128gb",
                    )
                )
                self.assertFalse(
                    has_investible_product_identity(
                        title,
                        source="ebay-apple",
                        model_key="apple-iphone-14-pro-128gb",
                    )
                )

    def test_unlocked_trading_specifics_do_not_block_iphone_14_pro_candidate(self):
        attrs = {
            "Model": "Apple iPhone 14 Pro",
            "Network": "Unlocked",
            "Lock Status": "Network Unlocked",
            "Storage Capacity": "128 GB",
        }

        self.assertTrue(
            has_investible_product_identity(
                "Apple iPhone 14 Pro 128GB, Space Black, Unlocked",
                attrs,
                source="ebay-apple",
                model_key="apple-iphone-14-pro-128gb",
            )
        )

    def test_macbook_screen_for_listing_is_device_part(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Original SCREEN for Apple MacBook Pro 14 2024 M4 A3401 Retina LCD",
                source="ebay-apple",
                model_key="apple-macbook-pro-142in-m4_b",
            )
        )

    def test_macbook_empty_original_box_is_not_device_comp(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple MacBook Pro 13.3 Empty Original Box & Inserts | Model A1502",
                source="ebay-apple",
                model_key="apple-macbook-pro-133in-intel_d",
            )
        )

    def test_macbook_touch_id_issue_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Apple MacBook Air 13 2020 M1 8GB RAM 256GB SSD - Touch ID Issue",
                source="ebay-apple",
                model_key="apple-macbook-air-133in-m1-8gb-8gb_b",
            )
        )

    def test_macbook_screen_and_mic_issue_is_review_only(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                'Boxed MacBook Pro A2338, 13.3" | M2 | 16GB RAM | 512GB NVMe *SCREEN & MIC ISSUE*',
                source="ebay-apple",
                model_key="apple-macbook-pro-13in-m2-16gb-512gb_b",
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
                model_key="apple-macbook-air-13in-m1-8gb-256gb",
            )
        )

    def test_clean_intel_core_macbook_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                'Apple MacBook Pro A2141 16" i7 16GB 512GB Good Condition',
                source="ebay-apple",
                model_key="apple-macbook-pro-a2141-16in-core-i7-16gb-512gb",
            )
        )

    def test_old_broad_samsung_key_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "Samsung Galaxy S23 Ultra Unlocked",
                source="ebay-samsung",
                model_key="samsung-s23ultra_b",
            )
        )

    def test_samsung_key_with_condition_suffix_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "Samsung Galaxy A55 5G 128GB",
                source="ebay-samsung",
                model_key="samsung-galaxy-a55-128gb-5g_b",
            )
        )

    def test_clean_samsung_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "Samsung Galaxy S24 Ultra 256GB",
                source="ebay-samsung",
                model_key="samsung-galaxy-s24-ultra-256gb",
            )
        )

    def test_clean_samsung_tab_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "Samsung Galaxy Tab S9 Ultra 256GB Wi-Fi",
                source="ebay-samsung",
                model_key="samsung-galaxy-tab-s9-ultra-256gb-wifi",
            )
        )

    def test_old_console_key_with_condition_suffix_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "Sony PS5 Blu Ray Disk Edition 825GB With Controller and Leads",
                source="ebay-consoles",
                model_key="ps5_825gb_b",
            )
        )

    def test_broad_xbox_series_key_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "Microsoft Xbox Series Console With Controller",
                source="ebay-consoles",
                model_key="xbox_series_b",
            )
        )

    def test_clean_ps5_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "Sony PS5 Blu Ray Disk Edition 825GB With Controller and Leads",
                source="ebay-consoles",
                model_key="sony-playstation-5-disc-825gb",
            )
        )

    def test_ps3_game_is_not_investible_as_console(self):
        self.assertFalse(
            has_investible_product_identity(
                "Final Fantasy XIV A Realm Reborn PS3 Complete With Manual Tested And Working",
                source="ebay-consoles",
                model_key="sony-playstation-3",
            )
        )

    def test_clean_xbox_series_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "Microsoft Xbox Series X 1TB Console With Controller",
                source="ebay-consoles",
                model_key="microsoft-xbox-series-x-1tb",
            )
        )

    def test_impossible_xbox_series_x_512gb_key_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "Microsoft Xbox Series S 512GB Video Game Console - White",
                source="ebay-consoles",
                model_key="microsoft-xbox-series-x-512gb",
            )
        )

    def test_clean_switch_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "Nintendo Switch OLED Console",
                source="ebay-consoles",
                model_key="nintendo-switch-oled",
            )
        )

    def test_old_actioncam_key_with_condition_suffix_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "GoPro HERO12 Black Action Camera Accessory Bundle",
                source="ebay-actioncams",
                model_key="gopro-hero12_b",
            )
        )

    def test_clean_actioncam_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "GoPro HERO12 Black Action Camera Accessory Bundle",
                source="ebay-actioncams",
                model_key="gopro-hero12",
            )
        )

    def test_broad_actioncam_key_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "GoPro HERO7 Action Camera Bundle",
                source="ebay-actioncams",
                model_key="gopro-hero7",
            )
        )

    def test_old_lego_key_with_condition_suffix_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "LEGO Creator 3in1 Cute Hamster with a Flower Toy",
                source="ebay-lego",
                model_key="lego-31376_b-c",
            )
        )

    def test_clean_lego_set_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "LEGO Creator 3in1 Cute Hamster with a Flower Toy",
                source="ebay-lego",
                model_key="lego-set-31376",
            )
        )

    def test_incomplete_lego_set_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "LEGO Star Wars A-Wing Starfighter 75003 Instructions No Mini figures Nearly Complete",
                source="ebay-lego",
                model_key="lego-set-75003",
            )
        )

    def test_clean_lego_minifig_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "Lego Black Classic Space Minifigure sp003",
                source="ebay-lego",
                model_key="lego-minifig-space-003",
            )
        )

    def test_old_motors_key_with_condition_suffix_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "2014 BMW 1 Series 118d M Sport Manual Diesel Hatchback",
                source="ebay-motors",
                model_key="bmw-1series118d-2.0l-2014-80k_b",
            )
        )

    def test_clean_motors_vehicle_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "2014 BMW 1 Series 118d M Sport Manual Diesel Hatchback",
                source="ebay-motors",
                model_key="vehicle-bmw-1series118d-2.0l-diesel-manual-hatchback-2014",
            )
        )

    def test_motors_part_key_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "PORSCHE 911 992 Front Right Bumper Air Inlet Trim 992807764A041",
                source="ebay-motors",
                model_key="vehicle-porsche-911-2018",
            )
        )

    def test_clean_gpu_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "MSI RTX 4070 Ti Super 16GB Graphics Card",
                source="ebay-graphics-cards",
                model_key="nvidia-rtx-4070-ti-super-16gb",
            )
        )

    def test_gpu_accessory_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "RTX 4090 Water Block Cooler Only",
                source="ebay-graphics-cards",
                model_key="nvidia-rtx-4090-24gb",
            )
        )

    def test_clean_cpu_key_is_investible_identity(self):
        self.assertTrue(
            has_investible_product_identity(
                "Intel Core i7-14700KF Desktop CPU",
                source="ebay-cpus",
                model_key="intel-core-i7-14700kf",
            )
        )

    def test_cpu_cooler_only_is_not_investible_identity(self):
        self.assertFalse(
            has_investible_product_identity(
                "AMD Ryzen 7 7800X3D Stock Cooler Only",
                source="ebay-cpus",
                model_key="amd-ryzen-7-7800x3d",
            )
        )

    def test_motherboards_are_collection_only_for_now(self):
        self.assertFalse(
            has_investible_product_identity(
                "MSI MAG B650 Tomahawk WiFi AM5 Motherboard",
                source="ebay-motherboards",
                model_key="msi-mag-b650-tomahawk-wifi",
            )
        )

    def test_camera_lenses_are_collection_only_for_now(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Sony FE 85mm F1.4 GM Lens Hood Only",
                source="ebay-camera-lenses",
                model_key="sony-fe-85mm-f14-gm",
            )
        )
        self.assertFalse(
            has_investible_product_identity(
                "Sony FE 85mm F1.4 GM Lens",
                source="ebay-camera-lenses",
                model_key="sony-fe-85mm-f14-gm",
            )
        )

    def test_industrial_automation_is_collection_only_for_now(self):
        self.assertTrue(
            looks_like_partial_or_accessory_listing(
                "Siemens S7 PLC Manual Book",
                source="ebay-industrial-automation",
                model_key="siemens-6es7-511-1ak02-0ab0",
            )
        )
        self.assertFalse(
            has_investible_product_identity(
                "Siemens SIMATIC S7-1500 CPU 6ES7 511-1AK02-0AB0",
                source="ebay-industrial-automation",
                model_key="siemens-6es7-511-1ak02-0ab0",
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

    def test_audit_switch_accessories_are_not_investible_consoles(self):
        examples = [
            (
                "Nintendo Switch 2 USB-C Wall Charger And Cable",
                {"Type": "USB Cable", "Compatible Product": "Nintendo Switch 2 Console"},
            ),
            (
                "Wireless Controller for Nintendo Switch 2",
                {"Type": "Controller", "Platform": "Nintendo Switch 2"},
            ),
            (
                "Nintendo Switch 2 Messenger Bag Carry Case",
                {"Type": "Carry Case", "Compatible Product": "Nintendo Switch 2 Console"},
            ),
            (
                "Nintendo Switch Console Game - Let's Build a Zoo -",
                {},
            ),
        ]

        for title, attrs in examples:
            with self.subTest(title=title):
                self.assertTrue(
                    looks_like_partial_or_accessory_listing(
                        title,
                        attrs,
                        source="ebay-consoles",
                        model_key="nintendo-switch-2",
                    )
                )
                self.assertFalse(
                    has_investible_product_identity(
                        title,
                        attrs,
                        source="ebay-consoles",
                        model_key="nintendo-switch-2",
                    )
                )

    def test_console_with_controller_is_still_investible_console(self):
        self.assertTrue(
            has_investible_product_identity(
                "Nintendo Switch 2 Console With Wireless Controller",
                source="ebay-consoles",
                model_key="nintendo-switch-2",
            )
        )

    def test_lego_promo_set_is_not_investible_as_full_set(self):
        self.assertFalse(
            has_investible_product_identity(
                "LEGO Castle Byers Exclusive Promo Set 75810",
                source="ebay-lego",
                model_key="lego-set-75810",
            )
        )


if __name__ == "__main__":
    unittest.main()
