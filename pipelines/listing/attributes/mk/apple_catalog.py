"""Supported Apple identity combinations, keyed by marketed family and variant."""

# https://support.apple.com/en-euro/108044
_PHONE_64 = '64gb 128gb 256gb'
_PHONE_128 = '128gb 256gb 512gb'
_PHONE_256 = '256gb 512gb 1tb'
IPHONE_STORAGE = {
    **dict.fromkeys(('8', '8-plus', 'xr', '11', '12', '12-mini', 'se-2', 'se-3'), _PHONE_64),
    **dict.fromkeys(('13', '13-mini', '14', '14-plus', '15', '15-plus',
                     '16', '16-plus', '16e'), _PHONE_128),
    **dict.fromkeys(('15-pro-max', '16-pro-max', '17-pro', '17-air'), _PHONE_256),
    **dict.fromkeys(('13-pro', '13-pro-max', '14-pro', '14-pro-max', '15-pro', '16-pro'),
                    _PHONE_128 + ' 1tb'),
    **dict.fromkeys(('11-pro', '11-pro-max', 'xs', 'xs-max'), '64gb 256gb 512gb'),
    **dict.fromkeys(('7', '7-plus'), '32gb 128gb 256gb'),
    **dict.fromkeys(('6', '6-plus', '6s', '6s-plus', 'se-1'), '16gb 32gb 64gb 128gb'),
    'x': '64gb 256gb',
    '12-pro': _PHONE_128,
    '12-pro-max': _PHONE_128,
    '17': '256gb 512gb',
    '17e': '256gb 512gb',
    '17-pro-max': _PHONE_256 + ' 2tb',
    '18-pro': _PHONE_256 + ' 2tb',
    '18-pro-max': _PHONE_256 + ' 2tb',
}

IPHONE_MODEL_NUMBERS = {
    '18-pro-max': 'a3473 a3716 a3717 a3718',
    '18-pro': 'a3472 a3713 a3714 a3715',
    '17e': 'a3575 a3634 a3635',
    '17-pro-max': 'a3257 a3525 a3526 a3527',
    '17-pro': 'a3256 a3522 a3523 a3524',
    '17': 'a3258 a3519 a3520 a3521',
    '17-air': 'a3260 a3516 a3517 a3518',
    '16e': 'a3212 a3408 a3409 a3410',
    '16-pro-max': 'a3084 a3295 a3296 a3297',
    '16-pro': 'a3083 a3292 a3293 a3294',
    '16-plus': 'a3082 a3289 a3290 a3291',
    '16': 'a3081 a3286 a3287 a3288',
    '15-pro-max': 'a2849 a3105 a3106 a3108',
    '15-pro': 'a2848 a3101 a3102 a3104',
    '15-plus': 'a2847 a3093 a3094 a3096',
    '15': 'a2846 a3089 a3090 a3092',
    '14-pro-max': 'a2651 a2893 a2894 a2895 a2896',
    '14-pro': 'a2650 a2889 a2890 a2891 a2892',
    '14-plus': 'a2632 a2885 a2886 a2887 a2888',
    '14': 'a2649 a2881 a2882 a2883 a2884',
    '13-pro-max': 'a2484 a2641 a2643 a2644 a2645',
    '13-pro': 'a2483 a2636 a2638 a2639 a2640',
    '13': 'a2482 a2631 a2633 a2634 a2635',
    '13-mini': 'a2481 a2626 a2628 a2629 a2630',
    '12-pro-max': 'a2342 a2410 a2411 a2412',
    '12-pro': 'a2341 a2406 a2407 a2408',
    '12': 'a2172 a2402 a2403 a2404',
    '12-mini': 'a2176 a2398 a2399 a2400',
    '11-pro': 'a2160 a2215 a2217',
    '11-pro-max': 'a2161 a2218 a2220',
    '11': 'a2111 a2221 a2223',
}

# https://support.apple.com/en-gb/108043
# family, ordinal generation, chip alias, canonical diagonal, capacities, Wi-Fi IDs, cellular IDs
_PRO = '256gb 512gb 1tb 2tb'
_PRO_EARLY_M = '128gb ' + _PRO
_IPAD_AIR = '128gb 256gb 512gb 1tb'
IPADS = (
    ('ipad', 11, 'a16', '11in', '128gb 256gb 512gb', 'a3354', 'a3355 a3356'),
    ('ipad', 10, 'a14', '109in', '64gb 256gb', 'a2696', 'a2757 a2777 a3162'),
    ('ipad', 9, 'a13', '102in', '64gb 256gb', 'a2602', 'a2603 a2604 a2605'),
    ('ipad', 8, 'a12', '102in', '32gb 128gb', 'a2270', 'a2428 a2429 a2430'),
    ('ipad', 7, 'a10', '102in', '32gb 128gb', 'a2197', 'a2198 a2200'),
    ('ipad', 6, 'a10', '97in', '32gb 128gb', 'a1893', 'a1954'),
    ('ipad', 5, 'a9', '97in', '32gb 128gb', 'a1822', 'a1823'),
    ('ipad', 4, 'a6x', '97in', '16gb 32gb 64gb 128gb', 'a1458', 'a1459 a1460'),
    ('ipad', 3, 'a5x', '97in', '16gb 32gb 64gb', 'a1416', 'a1403 a1430'),
    ('ipad', 2, 'a5', '97in', '16gb 32gb 64gb', 'a1395', 'a1396 a1397'),
    ('ipad', 1, 'a4', '97in', '16gb 32gb 64gb', 'a1219', 'a1337'),
    ('ipad-mini', 7, 'a17pro', '83in', '128gb 256gb 512gb', 'a2993', 'a2995 a2996'),
    ('ipad-mini', 6, 'a15', '83in', '64gb 256gb', 'a2567', 'a2568 a2569'),
    ('ipad-mini', 5, 'a12', '79in', '64gb 256gb', 'a2133', 'a2124 a2125 a2126'),
    ('ipad-mini', 4, 'a8', '79in', '16gb 32gb 64gb 128gb', 'a1538', 'a1550'),
    ('ipad-mini', 3, 'a7', '79in', '16gb 64gb 128gb', 'a1599', 'a1600'),
    ('ipad-mini', 2, 'a7', '79in', '16gb 32gb 64gb 128gb', 'a1489', 'a1490 a1491'),
    ('ipad-mini', 1, 'a5', '79in', '16gb 32gb 64gb', 'a1432', 'a1454 a1455'),
    ('ipad-air', 8, 'm4', '13in', _IPAD_AIR, 'a3461', 'a3462 a3464'),
    ('ipad-air', 8, 'm4', '11in', _IPAD_AIR, 'a3459', 'a3460 a3463'),
    ('ipad-air', 7, 'm3', '13in', _IPAD_AIR, 'a3268', 'a3269 a3271'),
    ('ipad-air', 7, 'm3', '11in', _IPAD_AIR, 'a3266', 'a3267 a3270'),
    ('ipad-air', 6, 'm2', '13in', _IPAD_AIR, 'a2898', 'a2899 a2900'),
    ('ipad-air', 6, 'm2', '11in', _IPAD_AIR, 'a2902', 'a2903 a2904'),
    ('ipad-air', 5, 'm1', '109in', '64gb 256gb', 'a2588', 'a2589 a2591'),
    ('ipad-air', 4, 'a14', '109in', '64gb 256gb', 'a2316', 'a2072 a2324 a2325'),
    ('ipad-air', 3, 'a12', '105in', '64gb 256gb', 'a2152', 'a2123 a2153 a2154'),
    ('ipad-air', 2, 'a8x', '97in', '16gb 32gb 64gb 128gb', 'a1566', 'a1567'),
    ('ipad-air', 1, 'a7', '97in', '16gb 32gb 64gb 128gb', 'a1474', 'a1475 a1476'),
    ('ipad-pro', 7, 'm5', '13in', _PRO, 'a3360', 'a3361 a3362'),
    ('ipad-pro', 6, 'm5', '11in', _PRO, 'a3357', 'a3358 a3359'),
    ('ipad-pro', 6, 'm4', '13in', _PRO, 'a2925', 'a2926 a3007'),
    ('ipad-pro', 5, 'm4', '11in', _PRO, 'a2836', 'a2837 a3006'),
    ('ipad-pro', 6, 'm2', '129in', _PRO_EARLY_M, 'a2436', 'a2437 a2764 a2766'),
    ('ipad-pro', 4, 'm2', '11in', _PRO_EARLY_M, 'a2759', 'a2435 a2761 a2762'),
    ('ipad-pro', 5, 'm1', '129in', _PRO_EARLY_M, 'a2378', 'a2379 a2461 a2462'),
    ('ipad-pro', 3, 'm1', '11in', _PRO_EARLY_M, 'a2377', 'a2301 a2459 a2460'),
    ('ipad-pro', 4, 'a12z', '129in', '128gb 256gb 512gb 1tb', 'a2229', 'a2069 a2232 a2233'),
    ('ipad-pro', 2, 'a12z', '11in', '128gb 256gb 512gb 1tb', 'a2228', 'a2068 a2230 a2231'),
    ('ipad-pro', 3, 'a12x', '129in', '64gb 256gb 512gb 1tb', 'a1876', 'a1895 a1983 a2014'),
    ('ipad-pro', 1, 'a12x', '11in', '64gb 256gb 512gb 1tb', 'a1980', 'a1934 a1979 a2013'),
    ('ipad-pro', 2, 'a10x', '129in', '64gb 256gb 512gb', 'a1670', 'a1671 a1821'),
    ('ipad-pro', 1, 'a10x', '105in', '64gb 256gb 512gb', 'a1701', 'a1709 a1852'),
    ('ipad-pro', 1, 'a9x', '97in', '32gb 128gb 256gb', 'a1673', 'a1674 a1675'),
    ('ipad-pro', 1, 'a9x', '129in', '32gb 128gb 256gb', 'a1584', 'a1652'),
)

# https://support.apple.com/en-gb/108056
# GPS and cellular model numbers, grouped by case size.
WATCH_MODELS = {
    'ultra': {49: ('', 'a2622 a2684 a2859')},
    'ultra-2': {49: ('', 'a2986 a2987')},
    'ultra-3': {49: ('', 'a3281 a3282')},
    'ultra-4': {49: ('', 'a3579 a3580')},
    'se': {40: ('a2351', 'a2353 a2355'), 44: ('a2352', 'a2354 a2356')},
    'se-2': {40: ('a2722', 'a2725 a2726 a2855'), 44: ('a2723', 'a2724 a2727 a2856')},
    'se-3': {40: ('a3324 a3391', 'a3326 a3327'), 44: ('a3325 a3392', 'a3328 a3329')},
    'series-1': {38: ('a1802', ''), 42: ('a1803', '')},
    'series-2': {38: ('a1757 a1816', ''), 42: ('a1758 a1817', '')},
    'series-3': {38: ('a1858', 'a1860 a1889 a1890'), 42: ('a1859', 'a1861 a1891 a1892')},
    'series-4': {40: ('a1977', 'a1975 a2007'), 44: ('a1978', 'a1976 a2008')},
    'series-5': {40: ('a2092', 'a2094 a2156'), 44: ('a2093', 'a2095 a2157')},
    'series-6': {40: ('a2291', 'a2293 a2375'), 44: ('a2292', 'a2294 a2376')},
    'series-7': {41: ('a2473', 'a2475 a2476'), 45: ('a2474', 'a2477 a2478')},
    'series-8': {41: ('a2770', 'a2772 a2773 a2857'), 45: ('a2771', 'a2774 a2775 a2858')},
    'series-9': {41: ('a2978', 'a2982 a2983'), 45: ('a2980', 'a2984 a2985')},
    'series-10': {42: ('a2997 a2998', 'a3001 a3002'), 46: ('a2999 a3000', 'a3003 a3206')},
    'series-11': {42: ('a3331 a3450', 'a3335 a3452'), 46: ('a3333 a3451', 'a3337 a3453')},
    'series-12': {42: ('a3581 a3583', 'a3582 a3586'), 46: ('a3584 a3585', 'a3587 a3588')},
}
WATCH_CELLULAR_MATERIALS = {
    2: 'aluminium steel ceramic', 3: 'aluminium steel ceramic',
    4: 'aluminium steel', 5: 'aluminium steel titanium ceramic',
    6: 'aluminium steel titanium', 7: 'aluminium steel titanium',
    8: 'aluminium steel', 9: 'aluminium steel',
    10: 'aluminium titanium', 11: 'aluminium titanium',
    12: 'aluminium titanium ceramic',
}

# https://support.apple.com/en-us/109525
# Keep compatible legacy spellings for proven single-connector products.
AIRPODS_MODELS = {
    '1': 'a1523 a1722', '2': 'a2032 a2031', '3': 'a2565 a2564',
    '4': 'a3053 a3050 a3054', '4-anc': 'a3056 a3055 a3057',
    '5': 'a3531 a3532 a3533', '5-wireless': 'a3439 a3440 a3441',
    'pro-1': 'a2084 a2083', 'pro-2-lightning': 'a2931 a2699 a2698',
    'pro-2-usbc': 'a3047 a3048 a3049', 'pro-3': 'a3063 a3064 a3065',
    'max-1-lightning': 'a2096', 'max-1-usbc': 'a3184', 'max-2': 'a3454',
}
AIRPODS_CASES = {
    'a1602', 'a1938', 'a2897', 'a2566', 'a3058', 'a3059', 'a2190',
    'a2700', 'a2968', 'a3122', 'a3908',
}

# https://support.apple.com/en-mide/102869 and the linked technical specifications.
# Chip and screen evidence identify these Air chassis even when the seller omits the code.
MACBOOK_AIRS = (
    ('a2337', 'm1', '133in', '13in 133in', '8gb 16gb'),
    ('a2681', 'm2', '136in', '13in 136in', '8gb 16gb 24gb'),
    ('a2941', 'm2', '153in', '15in 153in', '8gb 16gb 24gb'),
    ('a3113', 'm3', '136in', '13in 136in', '8gb 16gb 24gb'),
    ('a3114', 'm3', '153in', '15in 153in', '8gb 16gb 24gb'),
    ('a3240', 'm4', '136in', '13in 136in', '16gb 24gb 32gb'),
    ('a3241', 'm4', '153in', '15in 153in', '16gb 24gb 32gb'),
)

# https://support.apple.com/en-ie/102852, /108054, /102231 and their linked specs.
# One row per materially different GPU build. GPU detection is optional only when
# the other evidence uniquely identifies a build.
# family, model identifier, physical model aliases, screen, chip, GPU cores, RAM, storage
_MINI = '256gb 512gb 1tb 2tb'
_IMAC_MORE = '256gb 512gb 1tb 2tb'
_STUDIO = '512gb 1tb 2tb 4tb 8tb'
_ULTRA = '1tb 2tb 4tb 8tb'
_DESKTOP_STUDIO = (
    ('mac-studio', 'mac13-1', 'a2615', '', 'm1max', 24, '32gb 64gb', _STUDIO),
    ('mac-studio', 'mac13-1', 'a2615', '', 'm1max', 32, '32gb 64gb', _STUDIO),
    ('mac-studio', 'mac13-2', 'a2615', '', 'm1ultra', 48, '64gb 128gb', _ULTRA),
    ('mac-studio', 'mac13-2', 'a2615', '', 'm1ultra', 64, '64gb 128gb', _ULTRA),
    ('mac-studio', 'mac14-13', 'a2901', '', 'm2max', 30, '32gb 64gb', _STUDIO),
    ('mac-studio', 'mac14-13', 'a2901', '', 'm2max', 38, '32gb 64gb 96gb', _STUDIO),
    ('mac-studio', 'mac14-14', 'a2901', '', 'm2ultra', 60, '64gb 128gb 192gb', _ULTRA),
    ('mac-studio', 'mac14-14', 'a2901', '', 'm2ultra', 76, '64gb 128gb 192gb', _ULTRA),
    ('mac-studio', 'mac16-9', 'a3143', '', 'm4max', 32, '36gb', _STUDIO),
    ('mac-studio', 'mac16-9', 'a3143', '', 'm4max', 40, '48gb 64gb 128gb', _STUDIO),
    ('mac-studio', 'mac15-14', 'a3389', '', 'm3ultra', 60, '96gb 256gb', '1tb 2tb 4tb 8tb 16tb'),
    ('mac-studio', 'mac15-14', 'a3389', '', 'm3ultra', 80, '96gb 256gb', '1tb 2tb 4tb 8tb 16tb'),
)
MAC_DESKTOPS = (
    ('imac', 'imac21-1', 'a2438', '24in', 'm1', 8, '8gb 16gb', _IMAC_MORE),
    ('imac', 'imac21-2', 'a2439', '24in', 'm1', 7, '8gb 16gb', '256gb 512gb 1tb'),
    ('imac', 'imac21-2', 'a2439', '24in', 'm1', 8, '8gb 16gb', '256gb 512gb 1tb'),
    ('imac', 'mac15-4', 'a2873', '24in', 'm3', 8, '8gb 16gb 24gb', '256gb 512gb 1tb'),
    ('imac', 'mac15-5', 'a2874', '24in', 'm3', 10, '8gb 16gb 24gb', _IMAC_MORE),
    ('imac', 'mac16-2', 'a3247', '24in', 'm4', 8, '16gb 24gb', '256gb 512gb 1tb'),
    ('imac', 'mac16-3', 'a3248', '24in', 'm4', 10, '16gb 24gb 32gb', _IMAC_MORE),
    ('mac-mini', 'macmini9-1', 'a2348', '', 'm1', 8, '8gb 16gb', _MINI),
    ('mac-mini', 'mac14-3', 'a2686', '', 'm2', 10, '8gb 16gb 24gb', _MINI),
    ('mac-mini', 'mac14-12', 'a2816', '', 'm2pro', 16, '16gb 32gb', _STUDIO),
    ('mac-mini', 'mac14-12', 'a2816', '', 'm2pro', 19, '16gb 32gb', _STUDIO),
    ('mac-mini', 'mac16-10', 'a3238', '', 'm4', 10, '16gb 24gb 32gb', _MINI),
    ('mac-mini', 'mac16-11', 'a3239', '', 'm4pro', 16, '24gb 48gb 64gb', _STUDIO),
    ('mac-mini', 'mac16-11', 'a3239', '', 'm4pro', 20, '24gb 48gb 64gb', _STUDIO),
    ('mac-pro', 'mac14-8-tower', 'a2786', '', 'm2ultra', 60, '64gb 128gb 192gb', _ULTRA),
    ('mac-pro', 'mac14-8-tower', 'a2786', '', 'm2ultra', 76, '64gb 128gb 192gb', _ULTRA),
    ('mac-pro', 'mac14-8-rack', 'a2787', '', 'm2ultra', 60, '64gb 128gb 192gb', _ULTRA),
    ('mac-pro', 'mac14-8-rack', 'a2787', '', 'm2ultra', 76, '64gb 128gb 192gb', _ULTRA),
    *_DESKTOP_STUDIO,
)

# https://support.apple.com/en-gb/108052 and Apple's hardware model declarations.
# Groups of chassis, chip, diagonal, supported memory and SSD capacities.
_PRO_MAX_SSD = '512gb 1tb 2tb 4tb 8tb'
_PRO_BASIC_SSD = '256gb 512gb 1tb 2tb'
MACBOOK_PROS = (
    ('a2338', 'm1', '133in', '8gb 16gb', _PRO_BASIC_SSD),
    ('a2338', 'm2', '133in', '8gb 16gb 24gb', _PRO_BASIC_SSD),
    ('a2442 a2485', 'm1pro', '', '16gb 32gb', _PRO_MAX_SSD),
    ('a2442 a2485', 'm1max', '', '32gb 64gb', _PRO_MAX_SSD),
    ('a2779 a2780', 'm2pro', '', '16gb 32gb', _PRO_MAX_SSD),
    ('a2779 a2780', 'm2max', '', '32gb 64gb 96gb', _PRO_MAX_SSD),
    ('a2918', 'm3', '142in', '8gb 16gb 24gb', '512gb 1tb 2tb'),
    ('a2992 a2991', 'm3pro', '', '18gb 36gb', '512gb 1tb 2tb 4tb'),
    ('a2992 a2991', 'm3max', '', '36gb 48gb 64gb 96gb 128gb', _PRO_MAX_SSD),
    ('a3112', 'm4', '142in', '16gb 24gb 32gb', '512gb 1tb 2tb'),
    ('a3401 a3403', 'm4pro', '', '24gb 48gb 64gb', '512gb 1tb 2tb 4tb'),
    ('a3185 a3186', 'm4max', '', '36gb 48gb 64gb 128gb', '1tb 2tb 4tb 8tb'),
)


MACBOOK_PRO_GPU_CORES = {
    'm1': {8}, 'm1pro': {14, 16}, 'm1max': {24, 32},
    'm2': {10}, 'm2pro': {16, 19}, 'm2max': {30, 38},
    'm3': {10}, 'm3pro': {14, 18}, 'm3max': {30, 40},
    'm4': {10}, 'm4pro': {16, 20}, 'm4max': {32, 40},
}
