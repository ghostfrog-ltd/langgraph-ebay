<?php

use App\Actions\SyncPythonOpportunityEnrichment;
use App\Models\EbayListing;

test('stale opportunity enrichment is demoted when listing is not refreshed', function () {
    $stale = EbayListing::factory()->create([
        'ebay_id' => 'stale-opportunity',
        'pipeline_state' => 'opportunity',
        'current_price' => 22.00,
        'estimated_market_value' => 519.95,
        'llm_analysis' => 'Old analysis',
        'attributes_json' => [
            'roi_estimate' => 22.1,
            'max_bid' => 40,
            'comp_samples' => 8,
            'llm_verdict' => 'buy',
        ],
    ]);

    $fresh = EbayListing::factory()->create([
        'ebay_id' => 'fresh-opportunity',
        'pipeline_state' => 'opportunity',
        'current_price' => 100.00,
        'estimated_market_value' => 140.00,
        'llm_analysis' => 'Fresh analysis',
        'attributes_json' => [
            'roi_estimate' => 0.4,
        ],
    ]);

    $action = new class extends SyncPythonOpportunityEnrichment
    {
        public function cleanup(array $ids): void
        {
            $this->cleanupStaleEnrichment($ids);
        }
    };

    $action->cleanup(['fresh-opportunity']);

    $stale->refresh();
    $fresh->refresh();

    expect($stale->pipeline_state)->toBe('ingested')
        ->and((float) $stale->estimated_market_value)->toBe(22.0)
        ->and($stale->llm_analysis)->toBeNull()
        ->and($stale->attributes_json)->not->toHaveKey('roi_estimate')
        ->and($stale->attributes_json)->not->toHaveKey('max_bid')
        ->and($stale->attributes_json)->not->toHaveKey('comp_samples')
        ->and($stale->attributes_json)->not->toHaveKey('llm_verdict')
        ->and($fresh->pipeline_state)->toBe('opportunity')
        ->and($fresh->llm_analysis)->toBe('Fresh analysis');
});
