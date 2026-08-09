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
        'ebay-camera-lenses',
        'ebay-camera-lenses',
        'ebay',
        'https://www.ebay.co.uk/b/Camera-Lenses/3323',
        TRUE,
        (now() AT TIME ZONE 'utc'),
        (now() AT TIME ZONE 'utc'),
        'camera_lenses',
        3600
    ),
    (
        'ebay-industrial-automation',
        'ebay-industrial-automation',
        'ebay',
        'https://www.ebay.co.uk/sch/i.html?_nkw=industrial+automation',
        TRUE,
        (now() AT TIME ZONE 'utc'),
        (now() AT TIME ZONE 'utc'),
        'industrial_automation',
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
