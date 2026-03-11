<?php

use App\Livewire\PipelineStatus;
use App\Models\EbayListing;
use Livewire\Livewire;

test('pipeline status shows counts grouped by pipeline state', function () {
    EbayListing::factory()->create(['pipeline_state' => 'ingested']);
    EbayListing::factory()->count(2)->create(['pipeline_state' => 'analyzing']);
    EbayListing::factory()->count(3)->create(['pipeline_state' => 'opportunity']);
    EbayListing::factory()->count(4)->create(['pipeline_state' => 'dismissed']);

    Livewire::test(PipelineStatus::class)
        ->assertSee('Ingested')
        ->assertSee('1')
        ->assertSee('Analyzing')
        ->assertSee('2')
        ->assertSee('Opportunities')
        ->assertSee('3')
        ->assertSee('Dismissed')
        ->assertSee('4');
});

test('purge dismissed deletes only dismissed listings', function () {
    EbayListing::factory()->count(2)->create(['pipeline_state' => 'dismissed']);
    EbayListing::factory()->count(3)->create(['pipeline_state' => 'opportunity']);

    Livewire::test(PipelineStatus::class)
        ->call('purgeDismissed')
        ->assertSee('Dismissed')
        ->assertSee('0')
        ->assertSee('Opportunities')
        ->assertSee('3');

    expect(EbayListing::query()->where('pipeline_state', 'dismissed')->count())->toBe(0);
    expect(EbayListing::query()->where('pipeline_state', 'opportunity')->count())->toBe(3);
});

test('pipeline status shows the latest sync timestamp when available', function () {
    EbayListing::factory()->create([
        'last_synced_at' => now()->subMinute(),
    ]);

    Livewire::test(PipelineStatus::class)
        ->assertSee('Last Synced:')
        ->assertDontSee('Waiting for engine');
});
