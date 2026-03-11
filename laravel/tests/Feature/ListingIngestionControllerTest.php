<?php

use App\Models\EbayListing;

function pipelinePayload(array $overrides = []): array
{
    return array_merge([
        'ebay_id' => 'v1|123456789012|0',
        'title' => 'Apple MacBook Pro 14"',
        'current_price' => 799.99,
        'estimated_market_value' => 1029.99,
        'image_url' => 'https://example.com/image.jpg',
        'item_url' => 'https://www.ebay.co.uk/itm/123456789012',
        'llm_analysis' => 'Strong spread versus comps with clean title and low current bid pressure.',
        'pipeline_state' => 'opportunity',
    ], $overrides);
}

test('listing ingestion endpoint requires a valid pipeline token', function () {
    config()->set('services.pipeline.ingest_token', 'test-token');

    $this->postJson('/api/pipeline/listings', pipelinePayload())
        ->assertUnauthorized();
});

test('listing ingestion endpoint upserts listings by ebay id', function () {
    config()->set('services.pipeline.ingest_token', 'test-token');

    $headers = [
        'X-Pipeline-Token' => 'test-token',
    ];

    $this->postJson('/api/pipeline/listings', pipelinePayload(), $headers)
        ->assertOk()
        ->assertJsonPath('status', 'ok');

    $listing = EbayListing::query()->firstOrFail();

    expect($listing->title)->toBe('Apple MacBook Pro 14"');
    expect($listing->pipeline_state)->toBe('opportunity');
    expect($listing->last_synced_at)->not->toBeNull();

    $this->postJson('/api/pipeline/listings', pipelinePayload([
        'title' => 'Updated title from pipeline',
        'pipeline_state' => 'analyzing',
    ]), $headers)->assertOk();

    expect(EbayListing::query()->count())->toBe(1);
    expect($listing->fresh()->title)->toBe('Updated title from pipeline');
    expect($listing->fresh()->pipeline_state)->toBe('analyzing');
});
