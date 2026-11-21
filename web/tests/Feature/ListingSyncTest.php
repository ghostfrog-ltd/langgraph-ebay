<?php

namespace Tests\Feature;

use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Testing\TestResponse;
use Tests\TestCase;

class ListingSyncTest extends TestCase
{
    use RefreshDatabase;

    public function test_a_signed_snapshot_is_upserted_with_its_sold_evidence(): void
    {
        config(['services.ghostfrog_sync.secret' => 'test-secret']);
        $payload = $this->payload();
        $body = json_encode($payload, JSON_THROW_ON_ERROR);
        $timestamp = (string) now()->timestamp;
        $signature = hash_hmac('sha256', $timestamp.'.'.$body, 'test-secret');

        $response = $this->call(
            'POST',
            '/api/v1/sync/listings',
            server: [
                'CONTENT_TYPE' => 'application/json',
                'HTTP_ACCEPT' => 'application/json',
                'HTTP_X_GHOSTFROG_TIMESTAMP' => $timestamp,
                'HTTP_X_GHOSTFROG_SIGNATURE' => $signature,
            ],
            content: $body,
        );

        $response->assertOk()
            ->assertJsonPath('listings', 1)
            ->assertJsonPath('comps', 1);
        $this->assertDatabaseHas('published_listings', [
            'external_id' => '123456789012',
            'condition_grade' => 'C',
            'text_condition_grade' => 'B',
            'text_scan_grade' => 'D',
            'text_scan_checked' => true,
            'text_scan_review_required' => true,
            'visual_condition_grade' => 'C',
            'visual_condition_checked' => true,
            'visual_review_required' => true,
            'expected_profit' => 64.20,
        ]);
        $this->assertDatabaseHas('published_comps', [
            'listing_external_id' => '123456789012',
            'comp_external_id' => '999999999999',
            'final_price' => 499.00,
            'text_scan_grade' => 'B',
            'text_scan_checked' => true,
            'visual_condition_grade' => 'B',
            'visual_condition_checked' => true,
        ]);
    }

    public function test_a_signed_model_key_comp_snapshot_is_upserted(): void
    {
        config(['services.ghostfrog_sync.secret' => 'test-secret']);
        $payload = [
            'schema_version' => 1,
            'batch_id' => 'd7d7eb13-2ee0-448f-9690-06f3dcc744bc',
            'generated_at' => now()->toIso8601String(),
            'model_key_comps' => [[
                'source' => 'ebay-apple',
                'model_key' => 'apple-macbook-pro-a2442-14in-m1pro-16gb-512gb',
                'condition_grade' => 'B',
                'samples' => 89,
                'median_final_price' => 639.99,
                'computed_at' => now()->subHour()->toIso8601String(),
                'comps' => [[
                    'external_id' => '157855519619',
                    'source' => 'ebay-apple',
                    'title' => 'Apple MacBook Pro 14" A2442 2021 M1 Pro 16GB 512GB',
                    'url' => 'https://www.ebay.co.uk/itm/157855519619',
                    'sale_type' => 'bin',
                    'end_time' => now()->subDay()->toIso8601String(),
                    'final_price' => 639.99,
                    'bids_count' => 0,
                    'text_scan_grade' => 'B',
                    'text_scan_checked' => true,
                    'text_scan_review_required' => false,
                    'text_scan_risk_flags' => [],
                    'text_scan_model_name' => 'gemma3:12b',
                    'text_scan_checked_at' => now()->subMinute()->toIso8601String(),
                    'visual_condition_grade' => 'B',
                    'visual_condition_checked' => true,
                    'visual_review_required' => false,
                    'visual_damage_flags' => [],
                    'visual_model_name' => 'minicpm-v:8b',
                    'visual_checked_at' => now()->subMinute()->toIso8601String(),
                ]],
            ]],
        ];

        $body = json_encode($payload, JSON_THROW_ON_ERROR);
        $timestamp = (string) now()->timestamp;
        $signature = hash_hmac('sha256', $timestamp.'.'.$body, 'test-secret');

        $this->call(
            'POST',
            '/api/v1/sync/model-key-comps',
            server: [
                'CONTENT_TYPE' => 'application/json',
                'HTTP_ACCEPT' => 'application/json',
                'HTTP_X_GHOSTFROG_TIMESTAMP' => $timestamp,
                'HTTP_X_GHOSTFROG_SIGNATURE' => $signature,
            ],
            content: $body,
        )->assertOk()
            ->assertJsonPath('model_key_comps', 1)
            ->assertJsonPath('comp_rows', 1);

        $this->assertDatabaseHas('published_model_key_comps', [
            'model_key' => 'apple-macbook-pro-a2442-14in-m1pro-16gb-512gb',
            'condition_grade' => 'B',
            'samples' => 89,
            'median_final_price' => 639.99,
        ]);
        $this->assertDatabaseHas('published_model_key_comp_rows', [
            'comp_external_id' => '157855519619',
            'final_price' => 639.99,
            'text_scan_grade' => 'B',
            'text_scan_checked' => true,
            'visual_condition_grade' => 'B',
            'visual_condition_checked' => true,
        ]);
    }

    public function test_a_signed_sold_evidence_snapshot_is_upserted(): void
    {
        config(['services.ghostfrog_sync.secret' => 'test-secret']);
        $payload = [
            'schema_version' => 1,
            'batch_id' => 'bfc8cb0c-b357-4137-98b6-88eb08abe155',
            'generated_at' => now()->toIso8601String(),
            'full_snapshot' => true,
            'sold_evidence' => [[
                'external_id' => '157855519619',
                'source' => 'ebay-apple',
                'title' => 'Apple MacBook Pro 14" A2442 2021 M1 Pro 16GB 512GB',
                'url' => 'https://www.ebay.co.uk/itm/157855519619',
                'sale_type' => 'auction',
                'end_time' => now()->subDay()->toIso8601String(),
                'final_price' => 639.99,
                'bids_count' => 8,
                'condition_grade' => 'B',
                'model_key' => 'apple-macbook-pro-a2442-14in-m1pro-16gb-512gb',
                'text_scan_grade' => 'C',
                'text_scan_checked' => true,
                'text_scan_review_required' => true,
                'text_scan_risk_flags' => ['screen_damage'],
                'text_scan_model_name' => 'gemma3:12b',
                'text_scan_checked_at' => now()->subMinute()->toIso8601String(),
                'visual_condition_grade' => 'C',
                'visual_condition_checked' => true,
                'visual_review_required' => true,
                'visual_damage_flags' => ['screen_mark'],
                'visual_model_name' => 'minicpm-v:8b',
                'visual_checked_at' => now()->subMinute()->toIso8601String(),
            ]],
        ];

        $this->signedPost($payload, '/api/v1/sync/sold-evidence')
            ->assertOk()
            ->assertJsonPath('sold_evidence', 1);

        $this->assertDatabaseHas('published_sold_evidence', [
            'external_id' => '157855519619',
            'source' => 'ebay-apple',
            'final_price' => 639.99,
            'model_key' => 'apple-macbook-pro-a2442-14in-m1pro-16gb-512gb',
            'sync_batch_id' => 'bfc8cb0c-b357-4137-98b6-88eb08abe155',
            'text_scan_grade' => 'C',
            'text_scan_checked' => true,
            'text_scan_review_required' => true,
            'visual_condition_grade' => 'C',
            'visual_condition_checked' => true,
            'visual_review_required' => true,
        ]);
    }

    public function test_a_signed_3d_print_seller_snapshot_is_upserted(): void
    {
        config(['services.ghostfrog_sync.secret' => 'test-secret']);
        $payload = [
            'schema_version' => 1,
            'batch_id' => 'bb5d9123-8f2f-4c81-99ab-4855b76e1953',
            'generated_at' => now()->toIso8601String(),
            'marketplace' => 'EBAY_GB',
            'sellers' => [[
                'rank' => 1,
                'seller_username' => 'print_pioneers',
                'item_count' => 145,
                'active_value' => 7385.75,
                'max_price' => 139.95,
                'feedback_score' => 2256,
                'feedback_percentage' => 99.4,
                'matched_queries' => ['3d printed car', '3d printed rc'],
                'example_titles' => ['Plymouth Fury 1958 Model Car Kit 3D Printed'],
                'example_url' => 'https://www.ebay.co.uk/itm/127894837271',
            ]],
        ];

        $this->signedPost($payload, '/api/v1/sync/3d-print-sellers')
            ->assertOk()
            ->assertJsonPath('sellers', 1);

        $this->assertDatabaseHas('three_d_print_sellers', [
            'seller_username' => 'print_pioneers',
            'item_count' => 145,
            'active_value' => 7385.75,
            'sync_batch_id' => 'bb5d9123-8f2f-4c81-99ab-4855b76e1953',
        ]);
    }

    public function test_an_invalid_signature_is_rejected(): void
    {
        config(['services.ghostfrog_sync.secret' => 'test-secret']);
        $body = json_encode($this->payload(), JSON_THROW_ON_ERROR);

        $this->call(
            'POST',
            '/api/v1/sync/listings',
            server: [
                'CONTENT_TYPE' => 'application/json',
                'HTTP_ACCEPT' => 'application/json',
                'HTTP_X_GHOSTFROG_TIMESTAMP' => (string) now()->timestamp,
                'HTTP_X_GHOSTFROG_SIGNATURE' => 'wrong',
            ],
            content: $body,
        )->assertForbidden();

        $this->assertDatabaseCount('published_listings', 0);
    }

    public function test_chunked_snapshot_only_finalizes_on_the_final_chunk(): void
    {
        config(['services.ghostfrog_sync.secret' => 'test-secret']);
        $firstChunk = $this->payload([
            'batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'chunk' => ['index' => 1, 'total' => 2, 'final' => false],
        ]);
        $secondChunk = $this->payload([
            'batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'chunk' => ['index' => 2, 'total' => 2, 'final' => true],
            'listings' => [[
                ...$this->payload()['listings'][0],
                'external_id' => '222222222222',
                'title' => 'Second chunk listing',
                'comps' => [],
            ]],
        ]);

        $this->signedPost($firstChunk)
            ->assertOk()
            ->assertJsonPath('final', false)
            ->assertJsonPath('listings', 1);
        $this->assertDatabaseCount('sync_runs', 0);
        $this->assertDatabaseHas('published_listings', ['external_id' => '123456789012']);

        $this->signedPost($secondChunk)
            ->assertOk()
            ->assertJsonPath('final', true)
            ->assertJsonPath('listings', 2);
        $this->assertDatabaseCount('sync_runs', 1);
        $this->assertDatabaseHas('published_listings', ['external_id' => '222222222222']);
    }

    public function test_delta_sync_upserts_changed_rows_and_prunes_missing_ids(): void
    {
        config(['services.ghostfrog_sync.secret' => 'test-secret']);

        $unchangedButCurrent = [
            ...$this->payload()['listings'][0],
            'external_id' => '222222222222',
            'title' => 'Already-current listing',
            'current_price' => 200,
            'expected_profit' => 25,
            'comps' => [],
        ];
        $this->signedPost($this->payload([
            'batch_id' => '11111111-1111-4111-8111-111111111111',
            'listings' => [$this->payload()['listings'][0], $unchangedButCurrent],
        ]))->assertOk();

        $changed = [
            ...$this->payload()['listings'][0],
            'external_id' => '333333333333',
            'title' => 'New changed listing',
            'current_price' => 175,
            'expected_profit' => 45,
            'comps' => [],
        ];
        $delta = [
            'schema_version' => 1,
            'batch_id' => '33333333-3333-4333-8333-333333333333',
            'generated_at' => now()->toIso8601String(),
            'mode' => 'delta',
            'summary' => ['sync_mode' => 'delta', 'delta_upserts' => 1],
            'listings' => [$changed],
            'expired_external_ids' => ['123456789012'],
            'current_external_ids' => ['222222222222', '333333333333'],
        ];

        $this->signedPost($delta)
            ->assertOk()
            ->assertJsonPath('mode', 'delta')
            ->assertJsonPath('upserts', 1)
            ->assertJsonPath('expired', 1)
            ->assertJsonPath('listings', 2);

        $this->assertDatabaseMissing('published_listings', ['external_id' => '123456789012']);
        $this->assertDatabaseHas('published_listings', ['external_id' => '222222222222']);
        $this->assertDatabaseHas('published_listings', [
            'external_id' => '333333333333',
            'current_price' => 175,
            'expected_profit' => 45,
        ]);
    }

    public function test_delta_sync_allows_empty_upserts_for_id_only_prune(): void
    {
        config(['services.ghostfrog_sync.secret' => 'test-secret']);

        $this->signedPost($this->payload([
            'batch_id' => '11111111-1111-4111-8111-111111111111',
        ]))->assertOk();

        $delta = [
            'schema_version' => 1,
            'batch_id' => '44444444-4444-4444-8444-444444444444',
            'generated_at' => now()->toIso8601String(),
            'mode' => 'delta',
            'summary' => ['sync_mode' => 'delta', 'delta_upserts' => 0],
            'listings' => [],
            'expired_external_ids' => [],
            'current_external_ids' => [],
        ];

        $this->signedPost($delta)
            ->assertOk()
            ->assertJsonPath('mode', 'delta')
            ->assertJsonPath('upserts', 0)
            ->assertJsonPath('listings', 0);

        $this->assertDatabaseCount('published_listings', 0);
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    private function signedPost(array $payload, string $path = '/api/v1/sync/listings'): TestResponse
    {
        $body = json_encode($payload, JSON_THROW_ON_ERROR);
        $timestamp = (string) now()->timestamp;

        return $this->call(
            'POST',
            $path,
            server: [
                'CONTENT_TYPE' => 'application/json',
                'HTTP_ACCEPT' => 'application/json',
                'HTTP_X_GHOSTFROG_TIMESTAMP' => $timestamp,
                'HTTP_X_GHOSTFROG_SIGNATURE' => hash_hmac('sha256', $timestamp.'.'.$body, 'test-secret'),
            ],
            content: $body,
        );
    }

    /**
     * @param  array<string, mixed>  $overrides
     * @return array<string, mixed>
     */
    private function payload(array $overrides = []): array
    {
        return array_replace([
            'schema_version' => 1,
            'batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'generated_at' => now()->toIso8601String(),
            'summary' => ['checked' => 1, 'potentials' => 1],
            'listings' => [[
                'external_id' => '123456789012',
                'source' => 'ebay-apple',
                'title' => 'MacBook Air M2 16GB 512GB',
                'url' => 'https://www.ebay.co.uk/itm/123456789012',
                'model_key' => 'apple-macbook-air-m2-16gb-512gb',
                'condition_grade' => 'C',
                'text_condition_grade' => 'B',
                'text_scan_grade' => 'D',
                'text_scan_checked' => true,
                'text_scan_review_required' => true,
                'text_scan_risk_flags' => ['locked_device'],
                'text_scan_model_name' => 'gemma3:12b',
                'text_scan_checked_at' => now()->subMinute()->toIso8601String(),
                'visual_condition_grade' => 'C',
                'visual_condition_checked' => true,
                'visual_review_required' => true,
                'visual_damage_flags' => ['chipped_corner'],
                'visual_model_name' => 'minicpm-v:8b',
                'visual_checked_at' => now()->subMinute()->toIso8601String(),
                'sale_type' => 'auction',
                'end_time' => now()->addHour()->toIso8601String(),
                'current_price' => 350,
                'sold_median' => 500,
                'comp_samples' => 14,
                'max_safe_buy' => 364.20,
                'expected_profit' => 64.20,
                'expected_roi' => 0.1834,
                'price_gap_to_max_buy' => 14.20,
                'effective_min_comps' => 10,
                'signal' => 'confirmed same-grade sold evidence',
                'comps' => [[
                    'external_id' => '999999999999',
                    'source' => 'ebay-apple',
                    'title' => 'Sold MacBook Air M2',
                    'url' => 'https://www.ebay.co.uk/itm/999999999999',
                    'sale_type' => 'auction',
                    'end_time' => now()->subDay()->toIso8601String(),
                    'final_price' => 499,
                    'bids_count' => 12,
                    'condition_grade' => 'B',
                    'model_key' => 'apple-macbook-air-m2-16gb-512gb',
                    'text_scan_grade' => 'B',
                    'text_scan_checked' => true,
                    'text_scan_review_required' => false,
                    'text_scan_risk_flags' => [],
                    'text_scan_model_name' => 'gemma3:12b',
                    'text_scan_checked_at' => now()->subMinute()->toIso8601String(),
                    'visual_condition_grade' => 'B',
                    'visual_condition_checked' => true,
                    'visual_review_required' => false,
                    'visual_damage_flags' => [],
                    'visual_model_name' => 'minicpm-v:8b',
                    'visual_checked_at' => now()->subMinute()->toIso8601String(),
                ]],
            ]],
        ], $overrides);
    }
}
