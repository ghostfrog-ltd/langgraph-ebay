<?php

use App\Models\EbayListing;

test('roi percentage accessor returns the percentage uplift from current price to estimated market value', function () {
    $listing = EbayListing::factory()->make([
        'current_price' => 100.00,
        'estimated_market_value' => 130.00,
    ]);

    expect($listing->roi_percentage)->toBe(30.0);
});

test('roi percentage accessor returns zero when current price is zero', function () {
    $listing = EbayListing::factory()->make([
        'current_price' => 0.00,
        'estimated_market_value' => 130.00,
    ]);

    expect($listing->roi_percentage)->toBe(0.0);
});

test('hot opportunities scope returns only opportunity listings with roi above fifteen percent', function () {
    $hotListing = EbayListing::factory()->create([
        'pipeline_state' => 'opportunity',
        'current_price' => 100.00,
        'estimated_market_value' => 120.00,
    ]);

    EbayListing::factory()->create([
        'pipeline_state' => 'opportunity',
        'current_price' => 100.00,
        'estimated_market_value' => 110.00,
    ]);

    EbayListing::factory()->create([
        'pipeline_state' => 'dismissed',
        'current_price' => 100.00,
        'estimated_market_value' => 140.00,
    ]);

    $results = EbayListing::hotOpportunities()->get();

    expect($results)->toHaveCount(1);
    expect($results->first()->is($hotListing))->toBeTrue();
});
