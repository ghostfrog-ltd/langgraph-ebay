<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Third Party Services
    |--------------------------------------------------------------------------
    |
    | This file is for storing the credentials for third party services such
    | as Resend, Postmark, AWS, and more. This file provides the de facto
    | location for this type of information, allowing packages to have
    | a conventional file to locate the various service credentials.
    |
    */

    'postmark' => [
        'key' => env('POSTMARK_API_KEY'),
    ],

    'resend' => [
        'key' => env('RESEND_API_KEY'),
    ],

    'ses' => [
        'key' => env('AWS_ACCESS_KEY_ID'),
        'secret' => env('AWS_SECRET_ACCESS_KEY'),
        'region' => env('AWS_DEFAULT_REGION', 'us-east-1'),
    ],

    'slack' => [
        'notifications' => [
            'bot_user_oauth_token' => env('SLACK_BOT_USER_OAUTH_TOKEN'),
            'channel' => env('SLACK_BOT_USER_DEFAULT_CHANNEL'),
        ],
    ],

    'ghostfrog_sync' => [
        'secret' => env('GHOSTFROG_SYNC_SECRET'),
    ],

    'ghostfrog' => [
        'buy_signal_min_comps' => env('GF_HEARTBEAT_BUY_SIGNAL_MIN_COMPS', 50),
        'sources' => [
            'ebay-apple',
            'ebay-samsung',
            'ebay-graphics-cards',
            'ebay-cpus',
            'ebay-camera-lenses',
            'ebay-watches',
            'ebay-lego',
            'ebay-tools',
            'ebay-motherboards',
            'ebay-industrial-automation',
            'ebay-actioncams',
            'ebay-headphones',
            'ebay-pokemon',
            'ebay-consoles',
            'ebay-motors',
            'ebay-mini-cooper',
            'ebay-honda-msx125',
            'ebay-honda-nc750',
            'motomine',
            'ebay-retro-pc',
        ],
        'hidden_sources' => [
            'ebay-honda-nc750',
            'ebay-honda-msx125',
            'ebay-mini-cooper',
            'ebay-motors',
        ],
        'source_profit_gates' => [
            'ebay-consoles' => 20,
        ],
        'source_labels' => [
            'ebay-actioncams' => 'Action Cameras',
            'ebay-apple' => 'Apple',
            'ebay-camera-lenses' => 'Camera Lenses',
            'ebay-consoles' => 'Consoles',
            'ebay-cpus' => 'CPUs',
            'ebay-graphics-cards' => 'Graphics Cards',
            'ebay-headphones' => 'Headphones',
            'ebay-honda-msx125' => 'Honda MSX125',
            'ebay-honda-nc750' => 'Honda NC750',
            'ebay-industrial-automation' => 'Industrial Automation',
            'ebay-lego' => 'Lego',
            'ebay-mini-cooper' => 'Mini Cooper',
            'ebay-motherboards' => 'Motherboards',
            'ebay-motors' => 'Motors',
            'ebay-pokemon' => 'Pokémon',
            'ebay-retro-pc' => 'Retro PC',
            'ebay-samsung' => 'Samsung',
            'ebay-tools' => 'Tools',
            'ebay-watches' => 'Watches',
            'motomine' => 'Motomine',
        ],
    ],

    'google' => [
        'client_id' => env('GOOGLE_CLIENT_ID'),
        'client_secret' => env('GOOGLE_CLIENT_SECRET'),
        'redirect' => env('GOOGLE_REDIRECT_URI', '/auth/google/callback'),
    ],

];
