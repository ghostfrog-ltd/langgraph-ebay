<?php

use App\Livewire\AuctionIndex;
use App\Models\EbayListing;
use Livewire\Livewire;

test('auction index lists newest listings first', function () {
    $older = EbayListing::factory()->create([
        'title' => 'Older listing',
        'created_at' => now()->subHour(),
    ]);

    $newer = EbayListing::factory()->create([
        'title' => 'Newer listing',
        'created_at' => now(),
    ]);

    Livewire::test(AuctionIndex::class)
        ->assertSeeInOrder([
            $newer->title,
            $older->title,
        ]);
});

test('auction index can bulk dismiss selected rows', function () {
    $listings = EbayListing::factory()->count(2)->create([
        'pipeline_state' => 'ingested',
    ]);

    Livewire::test(AuctionIndex::class)
        ->set('selectedIds', $listings->pluck('id')->all())
        ->call('bulkDismiss')
        ->assertSet('selectedIds', []);

    expect(EbayListing::query()->where('pipeline_state', 'dismissed')->count())->toBe(2);
});

test('auction index can bulk reanalyze selected rows', function () {
    $listings = EbayListing::factory()->count(2)->create([
        'pipeline_state' => 'dismissed',
    ]);

    Livewire::test(AuctionIndex::class)
        ->set('selectedIds', $listings->pluck('id')->all())
        ->call('bulkReanalyze')
        ->assertSet('selectedIds', []);

    expect(EbayListing::query()->where('pipeline_state', 'analyzing')->count())->toBe(2);
});

test('auction index opens a selected listing for quick look', function () {
    $listing = EbayListing::factory()->create();

    Livewire::test(AuctionIndex::class)
        ->call('openListing', $listing->id)
        ->assertSet('selectedListingId', $listing->id)
        ->assertSee($listing->title);
});
