<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\EbayListing;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Validation\Rule;

class ListingIngestionController extends Controller
{
    public function store(Request $request): JsonResponse
    {
        $payload = $request->validate([
            'ebay_id' => ['required', 'string'],
            'source_platform' => ['nullable', 'string'],
            'category_name' => ['nullable', 'string'],
            'title' => ['required', 'string'],
            'current_price' => ['required', 'numeric'],
            'estimated_market_value' => ['required', 'numeric'],
            'image_url' => ['nullable', 'string'],
            'item_url' => ['required', 'string'],
            'llm_analysis' => ['nullable', 'string'],
            'attributes_json' => ['nullable', 'array'],
            'pipeline_state' => ['required', Rule::in(['ingested', 'analyzing', 'opportunity', 'dismissed'])],
        ]);

        $listing = EbayListing::query()->updateOrCreate(
            ['ebay_id' => $payload['ebay_id']],
            [
                ...$payload,
                'last_synced_at' => now(),
            ],
        );

        return response()->json([
            'status' => 'ok',
            'id' => $listing->id,
            'ebay_id' => $listing->ebay_id,
        ]);
    }
}
