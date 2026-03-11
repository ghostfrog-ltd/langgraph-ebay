<?php

namespace App\Actions;

use App\Models\EbayListing;
use Carbon\Carbon;
use Carbon\CarbonInterface;
use Illuminate\Support\Facades\DB;

class SyncPythonAuctionListings
{
    public function run(): int
    {
        $count = 0;

        $this->baseQuery()
            ->orderBy('id')
            ->chunkById(500, function ($rows) use (&$count): void {
                foreach ($rows as $row) {
                    $this->upsertListing($row);
                    $count++;
                }
            }, 'id');

        return $count;
    }

    protected function baseQuery()
    {
        return DB::connection('python_pgsql')
            ->table('auction_listings')
            ->select([
                'id',
                'external_id',
                'source',
                'title',
                'price_current',
                'url',
                'status',
                'first_seen',
                'last_seen_at',
                'time_left_s',
                'end_time',
                'notes',
                'model_key',
            ])
            ->whereNotNull('external_id')
            ->whereNotNull('title');
    }

    protected function upsertListing(object $row): void
    {
        $listing = EbayListing::query()->firstOrNew([
            'ebay_id' => (string) $row->external_id,
        ]);

        $listing->source_platform = $this->resolveSourcePlatform($row);
        $listing->category_name = $row->source ?: null;
        $listing->title = (string) $row->title;
        $listing->image_url = $listing->image_url;
        $listing->current_price = $this->asMoney($row->price_current);
        $listing->estimated_market_value = $listing->estimated_market_value ?? $listing->current_price;
        $listing->item_url = (string) ($row->url ?: 'https://www.ebay.co.uk/');
        $listing->llm_analysis = $listing->llm_analysis;
        $listing->attributes_json = $this->buildAttributesPayload($row, $listing);
        $listing->pipeline_state = $listing->pipeline_state ?: 'ingested';
        $listing->last_synced_at = now();

        if (! $listing->exists && $row->first_seen) {
            $listing->created_at = $this->parseTimestamp($row->first_seen);
        }

        $listing->updated_at = $this->parseTimestamp($row->last_seen_at) ?? now();
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

    protected function buildAttributesPayload(object $row, EbayListing $listing): array
    {
        $existing = is_array($listing->attributes_json) ? $listing->attributes_json : [];

        return array_filter([
            ...$existing,
            'source' => $row->source ?: ($existing['source'] ?? null),
            'status' => $row->status ?: ($existing['status'] ?? null),
            'model_key' => $row->model_key ?: ($existing['model_key'] ?? null),
            'notes' => $row->notes ?: ($existing['notes'] ?? null),
            'time_left_seconds' => $row->time_left_s ?? ($existing['time_left_seconds'] ?? null),
            'end_time' => $this->parseTimestamp($row->end_time)?->toIso8601String() ?? ($existing['end_time'] ?? null),
        ], fn ($value) => $value !== null && $value !== '');
    }

    protected function asMoney(mixed $value): float
    {
        return round((float) ($value ?? 0), 2);
    }

    protected function parseTimestamp(mixed $value): ?CarbonInterface
    {
        if (! $value) {
            return null;
        }

        return Carbon::parse($value);
    }
}
