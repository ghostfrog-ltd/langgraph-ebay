INSERT INTO sources (
    name,
    domain,
    type,
    base_url,
    enabled,
    created_at_utc,
    updated_at_utc,
    niche,
    scrape_interval_seconds
)
VALUES
    (
        'ebay-graphics-cards',
        'ebay-graphics-cards',
        'ebay',
        'https://www.ebay.co.uk/b/Graphics-Video-Cards/27386',
        TRUE,
        (now() AT TIME ZONE 'utc'),
        (now() AT TIME ZONE 'utc'),
        'graphics_cards',
        3600
    ),
    (
        'ebay-cpus',
        'ebay-cpus',
        'ebay',
        'https://www.ebay.co.uk/b/CPUs-Processors/164',
        TRUE,
        (now() AT TIME ZONE 'utc'),
        (now() AT TIME ZONE 'utc'),
        'cpus',
        3600
    ),
    (
        'ebay-motherboards',
        'ebay-motherboards',
        'ebay',
        'https://www.ebay.co.uk/b/Motherboards/51197',
        TRUE,
        (now() AT TIME ZONE 'utc'),
        (now() AT TIME ZONE 'utc'),
        'motherboards',
        3600
    )
ON CONFLICT (name) DO UPDATE
SET domain = EXCLUDED.domain,
    type = EXCLUDED.type,
    base_url = EXCLUDED.base_url,
    enabled = TRUE,
    updated_at_utc = (now() AT TIME ZONE 'utc'),
    niche = EXCLUDED.niche,
    scrape_interval_seconds = EXCLUDED.scrape_interval_seconds;
