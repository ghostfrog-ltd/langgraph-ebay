<?php

use App\Livewire\OpportunityCard;
use App\Livewire\OpportunityFeed;
use App\Models\EbayListing;
use Livewire\Livewire;

test('opportunity feed orders hot opportunities by highest roi first', function () {
    $higherRoi = EbayListing::factory()->create([
        'title' => 'Higher ROI Listing',
        'pipeline_state' => 'opportunity',
        'current_price' => 100.00,
        'estimated_market_value' => 150.00,
    ]);

    $lowerRoi = EbayListing::factory()->create([
        'title' => 'Lower ROI Listing',
        'pipeline_state' => 'opportunity',
        'current_price' => 100.00,
        'estimated_market_value' => 125.00,
    ]);

    EbayListing::factory()->create([
        'title' => 'Dismissed Listing',
        'pipeline_state' => 'dismissed',
        'current_price' => 100.00,
        'estimated_market_value' => 180.00,
    ]);

    Livewire::test(OpportunityFeed::class)
        ->assertSeeInOrder([
            $higherRoi->title,
            $lowerRoi->title,
        ])
        ->assertDontSee('Dismissed Listing');
});

test('opportunity feed shows three cards initially and loads five more on demand', function () {
    foreach (range(1, 9) as $index) {
        EbayListing::factory()->create([
            'title' => "Opportunity {$index}",
            'pipeline_state' => 'opportunity',
            'current_price' => 100.00,
            'estimated_market_value' => 200.00 - $index,
        ]);
    }

    Livewire::test(OpportunityFeed::class)
        ->assertSee('Opportunity 1')
        ->assertSee('Opportunity 2')
        ->assertSee('Opportunity 3')
        ->assertDontSee('Opportunity 4')
        ->assertSee('Load more')
        ->call('loadMore')
        ->assertSee('Opportunity 8')
        ->assertDontSee('Opportunity 9');
});

test('dismissing an opportunity card updates the listing state', function () {
    $listing = EbayListing::factory()->create([
        'pipeline_state' => 'opportunity',
        'current_price' => 100.00,
        'estimated_market_value' => 140.00,
    ]);

    Livewire::test(OpportunityCard::class, ['listing' => $listing])
        ->call('dismiss');

    expect($listing->fresh()->pipeline_state)->toBe('dismissed');
});

test('opportunity feed shows the listings widget totals', function () {
    EbayListing::factory()->count(2)->create([
        'last_synced_at' => now()->subMinutes(20),
    ]);

    EbayListing::factory()->create([
        'last_synced_at' => now()->subHours(2),
    ]);

    Livewire::test(OpportunityFeed::class)
        ->assertSee('Listings')
        ->assertSee('3')
        ->assertSee('2 synced in the last hour.');
});
