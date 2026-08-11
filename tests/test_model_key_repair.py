import unittest

from pipelines.listing.attributes.mk import normalise_model
from utils.condition import _derive_condition_grade


class ModelKeyRepairTests(unittest.TestCase):
    def test_apple_normalised_attrs_are_aliased(self):
        key = normalise_model(
            title="iPhone 11 64GB White Unlocked",
            attrs={"brand": "Apple", "model_name": "Apple iPhone 11", "storage_gb": 64},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-iphone-11-64gb")

    def test_macbook_bad_ram_attribute_does_not_duplicate_storage(self):
        key = normalise_model(
            title='Apple MacBook Air 13.6" M2 A2681 (2022) 16GB RAM 512GB SSD',
            attrs={
                "Brand": "Apple",
                "Model": "MacBook Air",
                "Series": "MacBook Air",
                "RAM Size": "512 GB",
                "Processor": "Apple M2",
                "Screen Size": "13.6 in",
                "Storage Capacity": "512 GB",
                "MPN": "A2681",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-air-a2681-136in-m2-16gb-512gb")

    def test_macbook_core_m3_is_not_apple_silicon_m3(self):
        key = normalise_model(
            title="Apple MacBook A1534 12inch Laptop M3-7Y32 8GB RAM 250GB SSD Ventura",
            attrs={
                "Brand": "Apple",
                "Model": "MacBook",
                "Series": "MacBook",
                "RAM Size": "8 GB",
                "Processor": "Intel Core M3",
                "Screen Size": "12 in",
                "SSD Capacity": "256 GB",
                "MPN": "A1534",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-a1534-12in-core-m3-8gb-256gb")

    def test_m3_pro_macbook_is_not_old_intel_core_m3(self):
        key = normalise_model(
            title='Apple MacBook Pro A2991 16.2" M3 PRO 36GB RAM 512GB SSD 18-Core GPU Grade B',
            attrs={
                "Brand": "Apple",
                "Model": "MacBook Pro",
                "Series": "MacBook Pro",
                "RAM Size": "36 GB",
                "Processor": "Apple M3 Pro",
                "Screen Size": "16.2 in",
                "SSD Capacity": "512 GB",
                "MPN": "A2991",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-pro-a2991-162in-m3pro-36gb-512gb")

    def test_macbook_16_point_0_screen_maps_to_16in(self):
        key = normalise_model(
            title='Apple MacBook Pro A2485 2021 16" M1 Pro 512GB 16GB',
            attrs={
                "Brand": "Apple",
                "Model": "MacBook Pro",
                "Series": "MacBook Pro",
                "RAM Size": "16 GB",
                "Processor": "Apple M1 Pro",
                "Screen Size": '16.0"',
                "SSD Capacity": "512 GB",
                "MPN": "A2485",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-pro-a2485-16in-m1pro-16gb-512gb")

    def test_title_only_iphone_can_get_strict_key(self):
        key = normalise_model(
            title="iPhone 15 Pro 256GB Blue Titanium, Unlocked",
            attrs={},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-iphone-15-pro-256gb")

    def test_iphone_air_is_not_plain_iphone_bucket(self):
        key = normalise_model(
            title="Apple iPhone 17 Air 256GB Sky Blue Unlocked UK Model",
            attrs={},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-iphone-17-air-256gb")

    def test_plain_iphone_17_stays_plain_iphone_bucket(self):
        key = normalise_model(
            title="Apple iPhone 17 256GB Black Unlocked UK Model",
            attrs={},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-iphone-17-256gb")

    def test_macbook_unlabelled_tb_storage_after_ram_is_used(self):
        key = normalise_model(
            title='2025 APPLE MACBOOK AIR 13" M4 10C/10C 16GB RAM 1TB MIDNIGHT SEALED',
            attrs={
                "Brand": "Apple",
                "Model": "MacBook Air",
                "Series": "MacBook Air",
                "RAM Size": "16 GB",
                "Processor": "Apple M4",
                "Screen Size": "13 in",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-air-136in-m4-16gb-1tb")

    def test_m2_macbook_air_13_seller_shorthand_maps_to_136in(self):
        key = normalise_model(
            title='Apple MacBook Air 13" 2022 M2 16GB 512GB',
            attrs={
                "Brand": "Apple",
                "Model": "MacBook Air",
                "Series": "MacBook Air",
                "RAM Size": "16 GB",
                "Processor": "Apple M2",
                "Screen Size": "13 in",
                "SSD Capacity": "512 GB",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-air-136in-m2-16gb-512gb")

    def test_macbook_ram_without_space_before_storage_does_not_become_512gb_ram(self):
        key = normalise_model(
            title="Apple MacBook Pro 16 2024 M4 Pro Chip 24GB RAM512GB SSD",
            attrs={
                "Brand": "Apple",
                "Model": "MacBook Pro",
                "Series": "MacBook Pro",
                "RAM Size": "24 GB",
                "Processor": "Apple M4 Pro",
                "Screen Size": "16 in",
                "SSD Capacity": "512 GB",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-pro-16in-m4pro-24gb-512gb")

    def test_macbook_bad_512gb_ram_title_uses_plausible_16gb_ram(self):
        key = normalise_model(
            title='MacBook Pro Touch Bar 15" 512GB RAM Intel Core i7 2.9GHz 16GB RAM',
            attrs={
                "Brand": "Apple",
                "Model": "A1707",
                "Series": "MacBook Pro",
                "RAM Size": "16 GB",
                "Processor": "Intel Core i7",
                "Screen Size": "15 in",
                "SSD Capacity": "512 GB",
                "MPN": "A1707",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-pro-a1707-15in-core-i7-16gb-512gb")

    def test_macbook_cpu_conflict_becomes_unknown(self):
        key = normalise_model(
            title="Apple MacBook Pro 16 2024 M4 Pro Chip 24GB RAM 512GB SSD",
            attrs={
                "Brand": "Apple",
                "Model": "MacBook Pro",
                "Series": "MacBook Pro",
                "RAM Size": "24 GB",
                "Processor": "Apple M3",
                "Screen Size": "16 in",
                "SSD Capacity": "512 GB",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "unknown")

    def test_macbook_title_line_beats_bad_series_attribute(self):
        key = normalise_model(
            title="MacBook Pro 13 2022 M2 Touch Bar 16GB / 512GB Parts Or Repair - Water Damage",
            attrs={
                "Brand": "Apple",
                "Model": "MacBook Air",
                "Series": "MacBook Air",
                "RAM Size": "16 GB",
                "Processor": "Apple M2",
                "Screen Size": "13 in",
                "SSD Capacity": "512 GB",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-macbook-pro-13in-m2-16gb-512gb")

    def test_water_damage_parts_or_repair_is_condition_d(self):
        grade = _derive_condition_grade(
            {"Condition": "Used"},
            "MacBook Pro 13 2022 M2 Touch Bar 16GB / 512GB Parts Or Repair - Water Damage",
        )

        self.assertEqual(grade, "D")

    def test_screen_damage_is_condition_c(self):
        grade = _derive_condition_grade(
            {},
            'Apple MacBook Pro A2485 16 inch M1 PRO 16GB RAM 512GB SSD Screen Damage',
        )

        self.assertEqual(grade, "C")

    def test_used_fair_is_condition_c(self):
        grade = _derive_condition_grade(
            {},
            "MacBook Pro A1707 (2017) i7 16GB | 256GB (RADEON PRO 555) GREY (USED FAIR)",
        )

        self.assertEqual(grade, "C")

    def test_console_white_light_error_is_condition_d(self):
        grade = _derive_condition_grade(
            {},
            "PS4 500GB Console Only Genuine white light error CUH-1116A",
        )

        self.assertEqual(grade, "D")

    def test_macbook_missing_exact_spec_becomes_unknown(self):
        key = normalise_model(
            title="Apple MacBook Air laptop good condition",
            attrs={"Brand": "Apple", "Model": "MacBook Air", "Series": "MacBook Air"},
            source="ebay-apple",
        )

        self.assertEqual(key, "unknown")

    def test_iphone_storage_only_key_becomes_unknown(self):
        key = normalise_model(
            title="Apple iPhone 256GB Unlocked",
            attrs={"Brand": "Apple", "Model": "iPhone", "Storage Capacity": "256 GB"},
            source="ebay-apple",
        )

        self.assertEqual(key, "unknown")

    def test_ps3_game_title_does_not_become_console_key(self):
        key = normalise_model(
            title="Final Fantasy XIV A Realm Reborn PS3 Complete With Manual Tested And Working",
            attrs={"Brand": "Sony"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_iphone_mini_does_not_mix_with_base_iphone(self):
        key = normalise_model(
            title="Apple iPhone 13 mini (PRODUCT)RED - 256GB - Unlocked",
            attrs={"Brand": "Apple", "Model": "Apple iPhone 13 mini", "Storage Capacity": "256 GB"},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-iphone-13-mini-256gb")

    def test_iphone_pro_max_keeps_tier_and_storage(self):
        key = normalise_model(
            title="Apple iPhone 15 Pro Max 512GB Natural Titanium Unlocked",
            attrs={"Brand": "Apple", "Model": "Apple iPhone 15 Pro Max", "Storage Capacity": "512 GB"},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-iphone-15-pro-max-512gb")

    def test_ipad_pro_m4_keeps_size_chip_storage_and_cellular(self):
        key = normalise_model(
            title="Apple iPad Pro (2024) M4 11-inch Tablet Cel 1TB Black WiFi",
            attrs={
                "Brand": "Apple",
                "Model": "iPad Pro",
                "Series": "iPad Pro",
                "Screen Size": "11 in",
                "Processor": "Apple M4",
            },
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-ipad-pro-11in-m4-1tb-cellular")

    def test_base_ipad_without_generation_becomes_unknown(self):
        key = normalise_model(
            title="Apple iPad 10.2 inch 64GB WiFi",
            attrs={"Brand": "Apple", "Model": "iPad", "Screen Size": "10.2 in", "Storage Capacity": "64 GB"},
            source="ebay-apple",
        )

        self.assertEqual(key, "unknown")

    def test_base_ipad_generation_storage_and_connectivity(self):
        key = normalise_model(
            title="Apple iPad 10th Generation 10.9 inch 64GB Wi-Fi Blue",
            attrs={"Brand": "Apple", "Model": "iPad 10th Generation", "Screen Size": "10.9 in"},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-ipad-109in-10th-gen-64gb-wifi")

    def test_apple_watch_series_size_and_gps(self):
        key = normalise_model(
            title="Apple Watch Series 9 45mm GPS Midnight Aluminium",
            attrs={"Brand": "Apple", "Model": "Apple Watch Series 9", "Case Size": "45 mm"},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-watch-series-9-45mm-gps")

    def test_airpods_pro_generation_and_connector(self):
        key = normalise_model(
            title="Apple AirPods Pro 2nd Generation USB-C with MagSafe Charging Case",
            attrs={"Brand": "Apple", "Model": "AirPods Pro 2"},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-airpods-pro-2-usbc")

    def test_airpods_case_only_is_unknown(self):
        key = normalise_model(
            title="Apple AirPods Pro Charging Case Only",
            attrs={"Brand": "Apple", "Model": "AirPods Pro"},
            source="ebay-apple",
        )

        self.assertEqual(key, "unknown")

    def test_apple_tv_4k_storage(self):
        key = normalise_model(
            title="Apple TV 4K 3rd Generation 128GB Wi-Fi + Ethernet",
            attrs={"Brand": "Apple", "Model": "Apple TV 4K", "Storage Capacity": "128 GB"},
            source="ebay-apple",
        )

        self.assertEqual(key, "apple-tv-4k-128gb")

    def test_samsung_s_ultra_keeps_tier_and_storage(self):
        key = normalise_model(
            title="Samsung Galaxy S24 Ultra 256GB Unlocked Titanium Grey",
            attrs={"Brand": "Samsung", "Model": "Samsung Galaxy S24 Ultra", "Storage Capacity": "256 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "samsung-galaxy-s24-ultra-256gb")

    def test_samsung_s_plus_keeps_plus_tier(self):
        key = normalise_model(
            title="Samsung Galaxy S25+ 512GB 5G Unlocked",
            attrs={"Brand": "Samsung", "Model": "Samsung Galaxy S25+", "Storage Capacity": "512 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "samsung-galaxy-s25-plus-512gb")

    def test_samsung_s_fe_keeps_fe_tier(self):
        key = normalise_model(
            title="Samsung Galaxy S25 FE 5G 256GB",
            attrs={"Brand": "Samsung", "Model": "Samsung Galaxy S25 FE 5G", "Storage Capacity": "256 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "samsung-galaxy-s25-fe-256gb")

    def test_samsung_storage_missing_becomes_unknown(self):
        key = normalise_model(
            title="Samsung Galaxy S23 Ultra Unlocked",
            attrs={"Brand": "Samsung", "Model": "Samsung Galaxy S23 Ultra"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "unknown")

    def test_samsung_fold_without_generation_becomes_unknown(self):
        key = normalise_model(
            title="Samsung Galaxy Fold 256GB",
            attrs={"Brand": "Samsung", "Model": "Samsung Fold", "Storage Capacity": "256 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "unknown")

    def test_samsung_z_fold_generation_and_storage(self):
        key = normalise_model(
            title="Samsung Galaxy Z Fold 6 512GB Unlocked",
            attrs={"Brand": "Samsung", "Model": "Z Fold 6", "Storage Capacity": "512 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "samsung-galaxy-z-fold6-512gb")

    def test_samsung_z_flip_model_number_maps_generation(self):
        key = normalise_model(
            title="Samsung Galaxy Z Flip 256GB",
            attrs={"Brand": "Samsung", "Model Number": "SM-F741B/DS", "Storage Capacity": "256 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "samsung-galaxy-z-flip6-256gb")

    def test_samsung_a_series_5g_is_kept_when_it_matters(self):
        key = normalise_model(
            title="Samsung Galaxy A55 5G 128GB Awesome Navy",
            attrs={"Brand": "Samsung", "Model": "Galaxy A55 5G", "Storage Capacity": "128 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "samsung-galaxy-a55-128gb-5g")

    def test_samsung_tab_keeps_generation_tier_storage_and_connectivity(self):
        key = normalise_model(
            title="Samsung Galaxy Tab S9 Ultra 256GB Wi-Fi Graphite",
            attrs={"Brand": "Samsung", "Model": "Galaxy Tab S9 Ultra", "Storage Capacity": "256 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "samsung-galaxy-tab-s9-ultra-256gb-wifi")

    def test_samsung_case_does_not_become_phone_key(self):
        key = normalise_model(
            title="Case for Samsung Galaxy S24 Ultra",
            attrs={"Brand": "Samsung", "Model": "Galaxy S24 Ultra"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "unknown")

    def test_non_samsung_in_samsung_source_is_unknown(self):
        key = normalise_model(
            title="Google Pixel 9 Pro 256GB",
            attrs={"Brand": "Google", "Model": "Google Pixel 9 Pro", "Storage Capacity": "256 GB"},
            source="ebay-samsung",
        )

        self.assertEqual(key, "unknown")

    def test_console_game_does_not_become_console_key(self):
        key = normalise_model(
            title="L.A Noire Game for Sony PS4 Playstation 4",
            attrs={"Platform": "Sony PlayStation 4", "Game Name": "L.A Noire"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_switch_accessory_kit_does_not_become_oled_console_key(self):
        key = normalise_model(
            title="10 in 1 Switch Sports Accessories Kit Bundle for Nintendo Switch Oled Game A5",
            attrs={"Platform": "Nintendo Switch"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_switch_joycon_pair_does_not_become_console_key(self):
        key = normalise_model(
            title="Nintendo Switch Joy-Con Controller Pair - Neon Red/Neon Blue 100% ORIGINAL",
            attrs={"Platform": "Nintendo Switch"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_switch_replacement_battery_does_not_become_console_key(self):
        key = normalise_model(
            title="BATTERY FOR NINTENDO SWITCH CONSOLE HAC-003 REPLACEMENT - NEW",
            attrs={},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_console_title_only_can_still_get_model_key(self):
        key = normalise_model(
            title="Nintendo Switch 32GB Console Only - Neon Blue/Red",
            attrs={},
            source="ebay-consoles",
        )

        self.assertEqual(key, "nintendo-switch-base")

    def test_playstation_one_title_does_not_use_ps3_attribute_key(self):
        key = normalise_model(
            title="Sony PlayStation 1 Console Bundle Controllers Cables & 3 Games",
            attrs={"Platform": "Sony PlayStation 3"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_console_hardware_from_normalised_attrs(self):
        key = normalise_model(
            title="Xbox Series X 1tb, Controller Is Power A Wired, No Box",
            attrs={"brand": "Microsoft", "model_name": "Microsoft Xbox Series X"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "microsoft-xbox-series-x-1tb")

    def test_xbox_series_s_storage_gets_separate_key(self):
        key = normalise_model(
            title="Microsoft Xbox Series S 512GB White Console",
            attrs={"Brand": "Microsoft", "Model": "Xbox Series S"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "microsoft-xbox-series-s-512gb")

    def test_xbox_series_s_title_beats_broad_xs_attribute(self):
        key = normalise_model(
            title="Microsoft Xbox Series S 512GB Video Game Console - White",
            attrs={"Brand": "Microsoft", "Model": "Microsoft Xbox Series X|S"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "microsoft-xbox-series-s-512gb")

    def test_broad_xbox_series_key_is_unknown(self):
        key = normalise_model(
            title="Microsoft Xbox Series Console With Controller",
            attrs={"Brand": "Microsoft", "Model": "Xbox Series"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "unknown")

    def test_switch_tablet_only_gets_separate_key(self):
        key = normalise_model(
            title="Nintendo Switch Console/tablet Only",
            attrs={"Platform": "Nintendo Switch"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "nintendo-switch-tablet-only")

    def test_switch_oled_ignores_bundle_storage(self):
        key = normalise_model(
            title="Nintendo Switch OLED Console With 128GB SD Card",
            attrs={"Platform": "Nintendo Switch"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "nintendo-switch-oled")

    def test_ps4_slim_storage_gets_separate_key(self):
        key = normalise_model(
            title="Sony PlayStation 4 PS4 Slim 500GB Console - Black",
            attrs={"Platform": "Sony PlayStation 4"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "sony-playstation-4-slim-500gb")

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

        self.assertEqual(key, "sony-playstation-5-disc-825gb")

    def test_ps5_slim_digital_storage_gets_separate_key(self):
        key = normalise_model(
            title="Sony PlayStation 5 Slim Digital Edition 1TB Console",
            attrs={"Platform": "Sony PlayStation 5"},
            source="ebay-consoles",
        )

        self.assertEqual(key, "sony-playstation-5-slim-digital-1tb")

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

        self.assertEqual(key, "lego-set-31376")

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

        self.assertEqual(key, "lego-set-21338")

    def test_lego_missing_minifigures_is_unknown(self):
        key = normalise_model(
            title="LEGO Star Wars A-Wing Starfighter 75003 Instructions No Mini figures Nearly Complete",
            attrs={"Brand": "LEGO", "MPN": "75003", "LEGO Theme": "Star Wars"},
            source="ebay-lego",
        )

        self.assertEqual(key, "unknown")

    def test_lego_multi_set_bundle_is_unknown(self):
        key = normalise_model(
            title="LEGO 10356 Star Trek USS Enterprise + 40768 Shuttlepod New & Sealed",
            attrs={"Brand": "LEGO"},
            source="ebay-lego",
        )

        self.assertEqual(key, "unknown")

    def test_lego_compatible_blocks_are_unknown(self):
        key = normalise_model(
            title="Pokemon Charizard Building Block Set LEGO Compatible 273pcs NEW Sealed",
            attrs={"Brand": "LEGO", "Type": "Complete Set"},
            source="ebay-lego",
        )

        self.assertEqual(key, "unknown")

    def test_lego_minifig_id_gets_minifig_key(self):
        key = normalise_model(
            title="Lego - Black Classic Space Minifigure with Airtanks - White Logo sp003",
            attrs={"Brand": "LEGO", "Item Type": "Minifigure"},
            source="ebay-lego",
        )

        self.assertEqual(key, "lego-minifig-space-003")

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

    def test_motors_vehicle_uses_manufacturer_model_and_specs(self):
        key = normalise_model(
            title="2014 BMW 1 Series 118d M Sport Manual Diesel Hatchback",
            attrs={
                "Manufacturer": "BMW",
                "Model": "1 Series 118d M Sport",
                "Engine Size": "2.0 L",
                "Fuel Type": "Diesel",
                "Transmission": "Manual",
                "Body Type": "5 Door Hatchback",
                "Year": "2014",
            },
            source="ebay-motors",
        )

        self.assertEqual(key, "vehicle-bmw-1series118d-2.0l-diesel-manual-hatchback-2014")

    def test_motors_make_alias_and_registration_date(self):
        key = normalise_model(
            title="Volkswagen Golf GT TDI Estate",
            attrs={
                "Manufacturer": "volgswagen",
                "Model": "Golf GT TDI",
                "Engine": "1.9 TDI",
                "Fuel Type": "Diesel",
                "Transmission": "Manual",
                "Body Type": "Estate",
                "Date of 1st Registration": "01032009",
            },
            source="ebay-motors",
        )

        self.assertEqual(key, "vehicle-volkswagen-golfgt-1.9l-diesel-manual-estate-2009")

    def test_motors_part_listing_is_unknown(self):
        key = normalise_model(
            title="PORSCHE 911 992 Front Right Bumper Air Inlet Trim 992807764A041",
            attrs={"Manufacturer": "Porsche", "Model": "911"},
            source="ebay-motors",
        )

        self.assertEqual(key, "unknown")

    def test_motors_non_vehicle_pollution_is_unknown(self):
        key = normalise_model(
            title="Daiwa fishing bivvy carp shelter",
            attrs={"Brand": "Daiwa", "Model": "Shelter"},
            source="ebay-motors",
        )

        self.assertEqual(key, "unknown")

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

        self.assertEqual(key, "gopro-hero12")

    def test_actioncam_structured_model_drops_brand_prefix(self):
        key = normalise_model(
            title="GoPro HERO12 Black Action Camera Accessory Bundle CHDRB121-A",
            attrs={"Brand": "GoPro", "Model": "GoPro HERO12", "Type": "Helmet/Action"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "gopro-hero12")

    def test_actioncam_title_beats_wrong_copied_model_attribute(self):
        key = normalise_model(
            title="GoPro HERO12 Black Action Camera & 3 Batteries & Charger",
            attrs={"Brand": "GoPro", "Model": "GoPro Hero 3+", "Type": "Helmet/Action"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "gopro-hero12")

    def test_actioncam_gopro_hero7_keeps_colour_variant(self):
        key = normalise_model(
            title="GoPro HERO7 Silver Action Camera Bundle",
            attrs={"Brand": "GoPro", "Model": "GoPro HERO7"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "gopro-hero7-silver")

    def test_actioncam_broad_gopro_hero7_is_unknown(self):
        key = normalise_model(
            title="GoPro HERO7 Action Camera Bundle",
            attrs={"Brand": "GoPro", "Model": "GoPro HERO7"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "unknown")

    def test_actioncam_dji_osmo_pocket_3_gets_key(self):
        key = normalise_model(
            title="DJI Osmo Pocket 3 Creator Combo 4K Ultra HD Action Camera",
            attrs={"Brand": "DJI", "Model": "Osmo Pocket 3"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "dji-osmo-pocket-3")

    def test_actioncam_dji_action_5_requires_pro_signal(self):
        key = normalise_model(
            title="DJI Osmo Action 5 Pro Adventure Combo",
            attrs={"Brand": "DJI", "Model": "Osmo Action 5"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "dji-osmo-action-5-pro")

    def test_actioncam_insta360_go_3s_keeps_storage(self):
        key = normalise_model(
            title="Insta360 GO 3S 128GB Standard Edition 4K Action Camera",
            attrs={"Brand": "Insta360", "Model": "GO 3S"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "insta360-go-3s-128gb")

    def test_actioncam_insta360_one_x2_gets_separate_key(self):
        key = normalise_model(
            title="Insta360 ONE X2 360 Degree Waterproof Action Camera",
            attrs={"Brand": "Insta360", "Model": "ONE X2"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "insta360-one-x2")

    def test_actioncam_akaso_model_gets_key(self):
        key = normalise_model(
            title="AKASO EK7000 Pro 4K Action Camera",
            attrs={"Brand": "AKASO", "Model": "EK7000 Pro"},
            source="ebay-actioncams",
        )

        self.assertEqual(key, "akaso-ek7000-pro")

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

    def test_gpu_rtx_4070_variants_do_not_merge(self):
        self.assertEqual(
            normalise_model("NVIDIA GeForce RTX 4070 12GB Graphics Card", {}, "ebay-graphics-cards"),
            "nvidia-rtx-4070-12gb",
        )
        self.assertEqual(
            normalise_model("MSI RTX 4070 Ti 12GB Gaming X Trio", {}, "ebay-graphics-cards"),
            "nvidia-rtx-4070-ti-12gb",
        )
        self.assertEqual(
            normalise_model("RTX 4070 Ti Super 16GB Graphics Card", {}, "ebay-graphics-cards"),
            "nvidia-rtx-4070-ti-super-16gb",
        )

    def test_gpu_rtx_3080_vram_variants_do_not_merge(self):
        self.assertEqual(
            normalise_model("NVIDIA RTX 3080 10GB Founders Edition", {}, "ebay-graphics-cards"),
            "nvidia-rtx-3080-10gb",
        )
        self.assertEqual(
            normalise_model("NVIDIA RTX 3080 12GB Graphics Card", {}, "ebay-graphics-cards"),
            "nvidia-rtx-3080-12gb",
        )
        self.assertEqual(
            normalise_model("NVIDIA RTX 3080 Graphics Card", {}, "ebay-graphics-cards"),
            "unknown",
        )

    def test_gpu_amd_xt_and_xtx_do_not_merge(self):
        self.assertEqual(
            normalise_model("AMD Radeon RX 7900 XT 20GB", {}, "ebay-graphics-cards"),
            "amd-radeon-rx-7900-xt-20gb",
        )
        self.assertEqual(
            normalise_model("Sapphire Radeon RX 7900 XTX 24GB", {}, "ebay-graphics-cards"),
            "amd-radeon-rx-7900-xtx-24gb",
        )

    def test_gpu_junk_is_unknown(self):
        self.assertEqual(
            normalise_model("RTX 4070 Ti Super Empty Box Only", {}, "ebay-graphics-cards"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("RTX 4090 Water Block Cooler Only", {}, "ebay-graphics-cards"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("RTX 3060 Laptop GPU MXM", {}, "ebay-graphics-cards"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("MSI GeForce GTX 2080 AERO 8GB Graphics Card", {}, "ebay-graphics-cards"),
            "unknown",
        )

    def test_damaged_gpu_keeps_condition_out_of_key(self):
        key = normalise_model("*DAMAGED* NVIDIA RTX 5090 32GB Graphics Card", {}, "ebay-graphics-cards")

        self.assertEqual(key, "nvidia-rtx-5090-32gb")
        self.assertEqual(_derive_condition_grade({}, "*DAMAGED* NVIDIA RTX 5090 32GB Graphics Card"), "C")

    def test_workstation_gpu_keys_are_supported(self):
        self.assertEqual(
            normalise_model("NVIDIA Quadro RTX 5000 16GB Graphics Card", {}, "ebay-graphics-cards"),
            "nvidia-quadro-rtx-5000-16gb",
        )
        self.assertEqual(
            normalise_model("NVIDIA RTX A4000 Graphics Card", {}, "ebay-graphics-cards"),
            "nvidia-rtx-a4000-16gb",
        )
        self.assertEqual(
            normalise_model("NVIDIA RTX 5000 Ada 32GB Workstation GPU", {}, "ebay-graphics-cards"),
            "nvidia-rtx-5000-ada-32gb",
        )
        self.assertEqual(
            normalise_model("AMD Radeon Pro W7900 48GB", {}, "ebay-graphics-cards"),
            "amd-radeon-pro-w7900-48gb",
        )

    def test_workstation_gpu_junk_is_unknown(self):
        self.assertEqual(
            normalise_model("NVIDIA RTX A4000 Laptop GPU", {}, "ebay-graphics-cards"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("NVIDIA RTX A5000 SXM Module", {}, "ebay-graphics-cards"),
            "unknown",
        )

    def test_cpu_suffixes_do_not_merge(self):
        self.assertEqual(
            normalise_model("AMD Ryzen 7 7800X3D Processor", {}, "ebay-cpus"),
            "amd-ryzen-7-7800x3d",
        )
        self.assertEqual(
            normalise_model("AMD Ryzen 7 7800X Processor", {}, "ebay-cpus"),
            "amd-ryzen-7-7800x",
        )
        self.assertEqual(
            normalise_model("Intel Core i7-14700K CPU", {}, "ebay-cpus"),
            "intel-core-i7-14700k",
        )
        self.assertEqual(
            normalise_model("Intel Core i7-14700KF CPU", {}, "ebay-cpus"),
            "intel-core-i7-14700kf",
        )

    def test_cpu_junk_is_unknown(self):
        self.assertEqual(
            normalise_model("Intel i7 14700K Empty Box Only", {}, "ebay-cpus"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("AMD Ryzen 7 7800X3D Stock Cooler Only", {}, "ebay-cpus"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("ASUS Motherboard Compatible With Ryzen 7 7800X3D", {}, "ebay-cpus"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("AMD Ryzen 5 7600X Bent Pins", {}, "ebay-cpus"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("Intel Core i7-4790K LGA1150 CPU", {}, "ebay-cpus"),
            "unknown",
        )

    def test_camera_lens_model_keys(self):
        self.assertEqual(
            normalise_model("Sony FE 85mm F1.4 GM Lens", {}, "ebay-camera-lenses"),
            "sony-fe-85mm-f14-gm",
        )
        self.assertEqual(
            normalise_model("Canon RF 85mm F1.2 L USM Lens", {}, "ebay-camera-lenses"),
            "canon-rf-85mm-f12-l",
        )
        self.assertEqual(
            normalise_model("Nikon Z 70-200mm F2.8 S Lens", {}, "ebay-camera-lenses"),
            "nikon-z-70-200mm-f28-s",
        )
        self.assertEqual(
            normalise_model("Sigma 85mm F1.4 DG DN Art Sony E Mount", {}, "ebay-camera-lenses"),
            "sigma-85mm-f14-dg-dn-art",
        )

    def test_camera_lens_accessory_is_unknown(self):
        self.assertEqual(
            normalise_model("Sony FE 85mm F1.4 GM Lens Hood Only", {}, "ebay-camera-lenses"),
            "unknown",
        )

    def test_industrial_automation_part_keys(self):
        self.assertEqual(
            normalise_model(
                "Siemens SIMATIC S7-1500 CPU 6ES7 511-1AK02-0AB0",
                {},
                "ebay-industrial-automation",
            ),
            "siemens-6es7-511-1ak02-0ab0",
        )
        self.assertEqual(
            normalise_model(
                "Allen Bradley PanelView Plus 2711P-T10C22D9P",
                {},
                "ebay-industrial-automation",
            ),
            "allen-bradley-2711p-t10c22d9p",
        )
        self.assertEqual(
            normalise_model("Beckhoff CX5130 Industrial PC PLC", {}, "ebay-industrial-automation"),
            "beckhoff-cx5130",
        )

    def test_industrial_automation_junk_is_unknown(self):
        self.assertEqual(
            normalise_model("Siemens S7 PLC Manual Book", {}, "ebay-industrial-automation"),
            "unknown",
        )

    def test_audit_switch_accessories_are_unknown_not_console_keys(self):
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
                self.assertEqual(normalise_model(title, attrs, "ebay-consoles"), "unknown")

    def test_console_with_controller_still_gets_console_key(self):
        self.assertEqual(
            normalise_model(
                "Nintendo Switch 2 Console With Wireless Controller",
                {"Type": "Video Game Console"},
                "ebay-consoles",
            ),
            "nintendo-switch-2",
        )

    def test_lego_promo_set_is_unknown_not_full_set_key(self):
        self.assertEqual(
            normalise_model("LEGO Castle Byers Exclusive Promo Set 75810", {}, "ebay-lego"),
            "unknown",
        )

    def test_pokemon_model_key_keeps_condition_out_of_key(self):
        key = normalise_model(
            "Bulbasaur 166/165 Sv2a: Pokemon Card 151 Art Rare Holo (Japanese)",
            {
                "Franchise": "Pokemon TCG",
                "Set": "SV2a: Pokemon Card 151",
                "Card Number": "166/165",
                "Language": "Japanese",
                "Condition": "Used",
            },
            "ebay-pokemon",
        )

        self.assertEqual(key, "pokemon-sv2apokemoncard151-166-165-holo_ja")
        self.assertNotRegex(key, r"_[a-d](-[a-d])?$")

    def test_pokemon_professional_grade_stays_in_key(self):
        key = normalise_model(
            "Pikachu Promo PSA 10 Pokemon Card",
            {
                "Franchise": "Pokemon TCG",
                "Set": "Promo Cards",
                "Card Name": "Pikachu",
                "Features": "Promo",
                "Language": "English",
                "Professional Grader": "PSA",
                "Condition": "Graded",
            },
            "ebay-pokemon",
        )

        self.assertEqual(key, "pokemon-promocards-pikachu-promo_en-psa10")

    def test_pokemon_mojibake_franchise_normalises_to_ascii(self):
        key = normalise_model(
            "Charizard 4/102 Holo PokÃ©mon TCG",
            {"Franchise": "PokÃ©mon TCG", "Card Number": "4/102", "Language": "English"},
            "ebay-pokemon",
        )

        self.assertEqual(key, "pokemon-004-102-holo_en")

    def test_pokemon_change_does_not_touch_apple_dispatch(self):
        key = normalise_model(
            "iPhone 11 64GB White Unlocked",
            {"brand": "Apple", "model_name": "Apple iPhone 11", "storage_gb": 64},
            "ebay-apple",
        )

        self.assertEqual(key, "apple-iphone-11-64gb")

    def test_motherboard_exact_names_do_not_merge(self):
        self.assertEqual(
            normalise_model("MSI MAG B650 Tomahawk WiFi AM5 Motherboard", {}, "ebay-motherboards"),
            "msi-mag-b650-tomahawk-wifi",
        )
        self.assertEqual(
            normalise_model("MSI MAG B650M Mortar WiFi AM5 Motherboard", {}, "ebay-motherboards"),
            "msi-mag-b650m-mortar-wifi",
        )
        self.assertEqual(
            normalise_model("ASUS ROG Strix B650E-F Gaming WiFi Motherboard", {}, "ebay-motherboards"),
            "asus-rog-strix-b650e-f-wifi",
        )
        self.assertEqual(
            normalise_model("Gigabyte B650 Aorus Elite AX V2 Motherboard", {}, "ebay-motherboards"),
            "gigabyte-b650-aorus-elite-ax-v2",
        )

    def test_motherboard_junk_or_broad_keys_are_unknown(self):
        self.assertEqual(
            normalise_model("ASUS Z790 Motherboard", {}, "ebay-motherboards"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("MSI MAG B650 Tomahawk WiFi Motherboard CPU RAM Bundle", {}, "ebay-motherboards"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("ASUS ROG Strix B650E-F Empty Box Only", {}, "ebay-motherboards"),
            "unknown",
        )
        self.assertEqual(
            normalise_model("MSI ROG STRIX Z790-F GAMING WIFI Intel i7 Bundle", {}, "ebay-motherboards"),
            "unknown",
        )


if __name__ == "__main__":
    unittest.main()
