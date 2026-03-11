<?php

namespace App\Actions;

use App\Models\EbayListing;
use Illuminate\Support\Facades\DB;

class SyncPythonOpportunityEnrichment
{
    public function run(): int
    {
        $count = 0;

        $this->baseQuery()
            ->orderBy('l.id')
            ->chunkById(250, function ($rows) use (&$count): void {
                foreach ($rows as $row) {
                    $this->upsertListing($row);
                    $count++;
                }
            }, 'l.id', 'id');

        return $count;
    }

    protected function baseQuery()
    {
        return DB::connection('python_pgsql')
            ->table('auction_listings as l')
            ->leftJoin('latest_comps as c', 'c.model_key', '=', 'l.model_key')
            ->leftJoin(DB::raw('LATERAL (
                SELECT
                    a.assessment,
                    a.verdict,
                    a.confidence,
                    a.recommended_max_bid,
                    a.created_at
                FROM listing_assessments as a
                WHERE a.listing_id = l.id
                ORDER BY a.created_at DESC
                LIMIT 1
            ) as la'), DB::raw('TRUE'), '=', DB::raw('TRUE'))
            ->select([
                'l.id',
                'l.external_id',
                'l.source',
                'l.status',
                'l.title',
                'l.url',
                'l.price_current',
                'l.roi_estimate',
                'l.max_bid',
                'l.notes',
                'l.model_key',
                'l.time_left_s',
                'l.end_time',
                'l.last_seen_at',
                'c.median_final_price',
                'c.samples as comp_samples',
                DB::raw('la.assessment::text as llm_analysis'),
                'la.verdict as llm_verdict',
                'la.confidence as llm_confidence',
                'la.recommended_max_bid as llm_recommended_max_bid',
                'la.created_at as assessment_created_at',
            ])
            ->whereNotNull('l.external_id')
            ->where(function ($query): void {
                $query->whereNotNull('l.roi_estimate')
                    ->orWhereNotNull(DB::raw('la.assessment'));
            });
    }

    protected function upsertListing(object $row): void
    {
        $listing = EbayListing::query()->firstOrNew([
            'ebay_id' => (string) $row->external_id,
        ]);

        $currentPrice = $this->asMoney($row->price_current);
        $estimatedMarketValue = $row->median_final_price !== null
            ? $this->asMoney($row->median_final_price)
            : ($listing->estimated_market_value !== null ? (float) $listing->estimated_market_value : $currentPrice);

        $listing->source_platform = $listing->source_platform ?: $this->resolveSourcePlatform($row);
        $listing->category_name = $listing->category_name ?: ($row->source ?: null);
        $listing->title = $listing->title ?: (string) $row->title;
        $listing->current_price = $currentPrice > 0 ? $currentPrice : $listing->current_price;
        $listing->estimated_market_value = $estimatedMarketValue;
        $listing->item_url = $listing->item_url ?: (string) ($row->url ?: 'https://www.ebay.co.uk/');
        $listing->llm_analysis = $row->llm_analysis ?: $listing->llm_analysis;
        $listing->attributes_json = $this->mergeAttributes($listing->attributes_json, $row);
        $listing->pipeline_state = $this->resolvePipelineState($row, $listing);
        $listing->last_synced_at = now();
        $listing->updated_at = now();

        if (! $listing->exists) {
            $listing->created_at = now();
        }

        $listing->save();
    }

    protected function resolveSourcePlatform(object $row): string
    {
        $url = (string) ($row->url ?? '');

        return match (true) {
            str_contains($url, 'ebay.co.uk') => 'eBay UK',
            str_contains($url, 'ebay.com') => 'eBay US',
            ! empty($row->source) => (string) $row->source,
            default => 'eBay',
        };
    }

    protected function resolvePipelineState(object $row, EbayListing $listing): string
    {
        $status = strtolower((string) ($row->status ?? ''));
        $roiEstimate = $row->roi_estimate !== null ? (float) $row->roi_estimate : null;

        if ($status === 'dismissed') {
            return 'dismissed';
        }

        if ($roiEstimate !== null && $roiEstimate > 0.15 && in_array($status, ['active', 'live', 'open', 'ending_soon'], true)) {
            return 'opportunity';
        }

        if ($row->llm_analysis !== null && in_array($status, ['active', 'live', 'open', 'ending_soon'], true)) {
            return 'analyzing';
        }

        return $listing->pipeline_state ?: 'ingested';
    }

    protected function mergeAttributes(mixed $existing, object $row): array
    {
        $existing = is_array($existing) ? $existing : [];

        return array_filter([
            ...$existing,
            'source' => $row->source ?: ($existing['source'] ?? null),
            'status' => $row->status ?: ($existing['status'] ?? null),
            'model_key' => $row->model_key ?: ($existing['model_key'] ?? null),
            'notes' => $row->notes ?: ($existing['notes'] ?? null),
            'time_left_seconds' => $row->time_left_s ?? ($existing['time_left_seconds'] ?? null),
            'end_time' => $row->end_time ?? ($existing['end_time'] ?? null),
            'roi_estimate' => $row->roi_estimate ?? ($existing['roi_estimate'] ?? null),
            'max_bid' => $row->max_bid ?? ($existing['max_bid'] ?? null),
            'comp_samples' => $row->comp_samples ?? ($existing['comp_samples'] ?? null),
            'llm_verdict' => $row->llm_verdict ?? ($existing['llm_verdict'] ?? null),
            'llm_confidence' => $row->llm_confidence ?? ($existing['llm_confidence'] ?? null),
            'llm_recommended_max_bid' => $row->llm_recommended_max_bid ?? ($existing['llm_recommended_max_bid'] ?? null),
            'assessment_created_at' => $row->assessment_created_at ?? ($existing['assessment_created_at'] ?? null),
        ], fn ($value) => $value !== null && $value !== '');
    }

    protected function asMoney(mixed $value): float
    {
        return round((float) ($value ?? 0), 2);
    }
}
