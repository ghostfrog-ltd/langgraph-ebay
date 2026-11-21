<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\PublishedComp;
use App\Models\PublishedListing;
use App\Models\SyncRun;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\ValidationException;
use Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException;

class ListingSyncController extends Controller
{
    public function __invoke(Request $request): JsonResponse
    {
        $this->verifySignature($request);
        $payload = $this->validatePayload($request->json()->all());
        $batchId = $payload['batch_id'];

        if ($existing = SyncRun::query()->where('batch_id', $batchId)->first()) {
            return response()->json([
                'batch_id' => $existing->batch_id,
                'listings' => $existing->listing_count,
                'comps' => $existing->comp_count,
                'duplicate' => true,
            ]);
        }

        if (($payload['mode'] ?? 'snapshot') === 'delta') {
            return $this->handleDelta($payload, $request, $batchId);
        }

        $isFinalChunk = $this->isFinalChunk($payload);
        $compCount = DB::transaction(function () use ($payload, $request, $batchId, $isFinalChunk): int {
            $compCount = 0;
            $listingRows = [];
            $compRows = [];
            $listingIds = [];
            $now = now();

            foreach ($payload['listings'] as $listingData) {
                $comps = $listingData['comps'] ?? [];
                unset($listingData['comps']);
                if (array_key_exists('visual_damage_flags', $listingData)) {
                    $listingData['visual_damage_flags'] = json_encode(
                        $listingData['visual_damage_flags'] ?? [],
                        JSON_THROW_ON_ERROR,
                    );
                }
                if (array_key_exists('text_scan_risk_flags', $listingData)) {
                    $listingData['text_scan_risk_flags'] = json_encode(
                        $listingData['text_scan_risk_flags'] ?? [],
                        JSON_THROW_ON_ERROR,
                    );
                }

                $listingIds[] = $listingData['external_id'];
                $listingRows[] = [
                    'external_id' => $listingData['external_id'],
                    ...Arr::only($listingData, $this->listingFields()),
                    'sync_batch_id' => $batchId,
                    'published_at' => $payload['generated_at'],
                    'created_at' => $now,
                    'updated_at' => $now,
                ];

                $seenComps = [];
                foreach ($comps as $comp) {
                    $compExternalId = (string) $comp['external_id'];
                    if (isset($seenComps[$compExternalId])) {
                        continue;
                    }
                    $seenComps[$compExternalId] = true;
                    if (array_key_exists('visual_damage_flags', $comp)) {
                        $comp['visual_damage_flags'] = json_encode(
                            $comp['visual_damage_flags'] ?? [],
                            JSON_THROW_ON_ERROR,
                        );
                    }
                    if (array_key_exists('text_scan_risk_flags', $comp)) {
                        $comp['text_scan_risk_flags'] = json_encode(
                            $comp['text_scan_risk_flags'] ?? [],
                            JSON_THROW_ON_ERROR,
                        );
                    }
                    $compRows[] = [
                        'listing_external_id' => $listingData['external_id'],
                        'comp_external_id' => $compExternalId,
                        ...Arr::only($comp, $this->compFields()),
                        'created_at' => $now,
                        'updated_at' => $now,
                    ];
                    $compCount++;
                }
            }

            foreach (array_chunk($listingRows, 500) as $chunk) {
                PublishedListing::query()->upsert(
                    $chunk,
                    ['external_id'],
                    [
                        ...$this->listingFields(),
                        'sync_batch_id',
                        'published_at',
                        'updated_at',
                    ],
                );
            }

            foreach (array_chunk($listingIds, 1000) as $chunk) {
                PublishedComp::query()->whereIn('listing_external_id', $chunk)->delete();
            }

            foreach (array_chunk($compRows, 1000) as $chunk) {
                DB::table('published_comps')->insert($chunk);
            }

            if ($isFinalChunk) {
                PublishedListing::query()->where('sync_batch_id', '!=', $batchId)->delete();
                SyncRun::query()->create([
                    'batch_id' => $batchId,
                    'generated_at' => $payload['generated_at'],
                    'received_at' => now(),
                    'listing_count' => PublishedListing::query()->where('sync_batch_id', $batchId)->count(),
                    'comp_count' => PublishedComp::query()
                        ->whereHas('listing', fn ($query) => $query->where('sync_batch_id', $batchId))
                        ->count(),
                    'payload_sha256' => hash('sha256', $request->getContent()),
                    'summary' => $payload['summary'] ?? null,
                ]);
            }

            return $compCount;
        });

        $listingCount = count($payload['listings']);
        $responseCompCount = $compCount;
        if ($isFinalChunk) {
            $syncRun = SyncRun::query()->where('batch_id', $batchId)->firstOrFail();
            $listingCount = $syncRun->listing_count;
            $responseCompCount = $syncRun->comp_count;
        }

        return response()->json([
            'batch_id' => $batchId,
            'listings' => $listingCount,
            'comps' => $responseCompCount,
            'duplicate' => false,
            'final' => $isFinalChunk,
        ]);
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    private function handleDelta(array $payload, Request $request, string $batchId): JsonResponse
    {
        $result = DB::transaction(function () use ($payload, $request, $batchId): array {
            $compCount = 0;
            $listingRows = [];
            $compRows = [];
            $upsertIds = [];
            $now = now();

            foreach ($payload['listings'] as $listingData) {
                $comps = $listingData['comps'] ?? [];
                unset($listingData['comps']);
                if (array_key_exists('visual_damage_flags', $listingData)) {
                    $listingData['visual_damage_flags'] = json_encode(
                        $listingData['visual_damage_flags'] ?? [],
                        JSON_THROW_ON_ERROR,
                    );
                }
                if (array_key_exists('text_scan_risk_flags', $listingData)) {
                    $listingData['text_scan_risk_flags'] = json_encode(
                        $listingData['text_scan_risk_flags'] ?? [],
                        JSON_THROW_ON_ERROR,
                    );
                }

                $upsertIds[] = $listingData['external_id'];
                $listingRows[] = [
                    'external_id' => $listingData['external_id'],
                    ...Arr::only($listingData, $this->listingFields()),
                    'sync_batch_id' => $batchId,
                    'published_at' => $payload['generated_at'],
                    'created_at' => $now,
                    'updated_at' => $now,
                ];

                $seenComps = [];
                foreach ($comps as $comp) {
                    $compExternalId = (string) $comp['external_id'];
                    if (isset($seenComps[$compExternalId])) {
                        continue;
                    }
                    $seenComps[$compExternalId] = true;
                    if (array_key_exists('visual_damage_flags', $comp)) {
                        $comp['visual_damage_flags'] = json_encode(
                            $comp['visual_damage_flags'] ?? [],
                            JSON_THROW_ON_ERROR,
                        );
                    }
                    if (array_key_exists('text_scan_risk_flags', $comp)) {
                        $comp['text_scan_risk_flags'] = json_encode(
                            $comp['text_scan_risk_flags'] ?? [],
                            JSON_THROW_ON_ERROR,
                        );
                    }
                    $compRows[] = [
                        'listing_external_id' => $listingData['external_id'],
                        'comp_external_id' => $compExternalId,
                        ...Arr::only($comp, $this->compFields()),
                        'created_at' => $now,
                        'updated_at' => $now,
                    ];
                    $compCount++;
                }
            }

            foreach (array_chunk($listingRows, 500) as $chunk) {
                PublishedListing::query()->upsert(
                    $chunk,
                    ['external_id'],
                    [
                        ...$this->listingFields(),
                        'sync_batch_id',
                        'published_at',
                        'updated_at',
                    ],
                );
            }

            foreach (array_chunk($upsertIds, 1000) as $chunk) {
                PublishedComp::query()->whereIn('listing_external_id', $chunk)->delete();
            }

            foreach (array_chunk($compRows, 1000) as $chunk) {
                DB::table('published_comps')->insert($chunk);
            }

            $expiredIds = collect($payload['expired_external_ids'] ?? [])
                ->map(fn ($id) => (string) $id)
                ->filter()
                ->unique()
                ->values()
                ->all();

            foreach (array_chunk($expiredIds, 1000) as $chunk) {
                PublishedListing::query()->whereIn('external_id', $chunk)->delete();
            }

            if (array_key_exists('current_external_ids', $payload)) {
                $currentIds = collect($payload['current_external_ids'] ?? [])
                    ->map(fn ($id) => (string) $id)
                    ->filter()
                    ->unique()
                    ->values()
                    ->all();

                if ($currentIds === []) {
                    PublishedListing::query()->delete();
                } else {
                    PublishedListing::query()->whereNotIn('external_id', $currentIds)->delete();
                }
            }

            $listingCount = PublishedListing::query()->count();
            $publishedCompCount = PublishedComp::query()->count();
            SyncRun::query()->create([
                'batch_id' => $batchId,
                'generated_at' => $payload['generated_at'],
                'received_at' => now(),
                'listing_count' => $listingCount,
                'comp_count' => $publishedCompCount,
                'payload_sha256' => hash('sha256', $request->getContent()),
                'summary' => $payload['summary'] ?? null,
            ]);

            return [
                'upserts' => count($payload['listings']),
                'expired' => count($expiredIds),
                'upsert_comps' => $compCount,
                'listings' => $listingCount,
                'comps' => $publishedCompCount,
            ];
        });

        return response()->json([
            'batch_id' => $batchId,
            'mode' => 'delta',
            'duplicate' => false,
            ...$result,
        ]);
    }

    private function verifySignature(Request $request): void
    {
        $secret = (string) config('services.ghostfrog_sync.secret');
        $timestamp = (string) $request->header('X-GhostFrog-Timestamp', '');
        $signature = (string) $request->header('X-GhostFrog-Signature', '');
        if ($secret === '' || ! ctype_digit($timestamp) || abs(time() - (int) $timestamp) > 300) {
            throw new AccessDeniedHttpException('Invalid sync signature.');
        }

        $expected = hash_hmac('sha256', $timestamp.'.'.$request->getContent(), $secret);
        if (! hash_equals($expected, $signature)) {
            throw new AccessDeniedHttpException('Invalid sync signature.');
        }
    }

    /** @param array<string, mixed> $payload
     * @return array<string, mixed>
     */
    private function validatePayload(array $payload): array
    {
        $validator = Validator::make($payload, [
            'schema_version' => ['required', 'integer', 'in:1'],
            'batch_id' => ['required', 'uuid'],
            'generated_at' => ['required', 'date'],
            'mode' => ['nullable', 'string', 'in:snapshot,delta'],
            'summary' => ['nullable', 'array'],
            'chunk' => ['nullable', 'array'],
            'chunk.index' => ['required_with:chunk', 'integer', 'min:1'],
            'chunk.total' => ['required_with:chunk', 'integer', 'min:1'],
            'chunk.final' => ['required_with:chunk', 'boolean'],
            'expired_external_ids' => ['nullable', 'array', 'max:50000'],
            'expired_external_ids.*' => ['string', 'max:255'],
            'current_external_ids' => ['nullable', 'array', 'max:50000'],
            'current_external_ids.*' => ['string', 'max:255'],
            'listings' => ['present', 'array', 'max:20000'],
            'listings.*.external_id' => ['required', 'string', 'max:64'],
            'listings.*.source' => ['required', 'string', 'max:100'],
            'listings.*.title' => ['required', 'string', 'max:1000'],
            'listings.*.url' => ['required', 'url', 'max:2000'],
            'listings.*.model_key' => ['required', 'string', 'max:500'],
            'listings.*.condition_grade' => ['required', 'string', 'max:4'],
            'listings.*.text_condition_grade' => ['nullable', 'string', 'max:4'],
            'listings.*.text_scan_grade' => ['nullable', 'string', 'max:16'],
            'listings.*.text_scan_checked' => ['nullable', 'boolean'],
            'listings.*.text_scan_review_required' => ['nullable', 'boolean'],
            'listings.*.text_scan_risk_flags' => ['nullable', 'array'],
            'listings.*.text_scan_risk_flags.*' => ['string', 'max:64'],
            'listings.*.text_scan_model_name' => ['nullable', 'string', 'max:255'],
            'listings.*.text_scan_checked_at' => ['nullable', 'date'],
            'listings.*.visual_condition_grade' => ['nullable', 'string', 'max:16'],
            'listings.*.visual_condition_checked' => ['nullable', 'boolean'],
            'listings.*.visual_review_required' => ['nullable', 'boolean'],
            'listings.*.visual_damage_flags' => ['nullable', 'array'],
            'listings.*.visual_damage_flags.*' => ['string', 'max:64'],
            'listings.*.visual_model_name' => ['nullable', 'string', 'max:255'],
            'listings.*.visual_checked_at' => ['nullable', 'date'],
            'listings.*.sale_type' => ['nullable', 'string', 'max:100'],
            'listings.*.end_time' => ['nullable', 'date'],
            'listings.*.current_price' => ['required', 'numeric'],
            'listings.*.sold_median' => ['required', 'numeric'],
            'listings.*.comp_samples' => ['required', 'integer', 'min:0'],
            'listings.*.max_safe_buy' => ['required', 'numeric'],
            'listings.*.expected_profit' => ['required', 'numeric'],
            'listings.*.expected_roi' => ['required', 'numeric'],
            'listings.*.price_gap_to_max_buy' => ['required', 'numeric'],
            'listings.*.effective_min_comps' => ['required', 'integer', 'min:1'],
            'listings.*.signal' => ['required', 'string', 'max:2000'],
            'listings.*.comps' => ['present', 'array', 'max:50'],
            'listings.*.comps.*.external_id' => ['required', 'string', 'max:64'],
            'listings.*.comps.*.source' => ['nullable', 'string', 'max:100'],
            'listings.*.comps.*.title' => ['required', 'string', 'max:1000'],
            'listings.*.comps.*.url' => ['nullable', 'url', 'max:2000'],
            'listings.*.comps.*.sale_type' => ['nullable', 'string', 'max:100'],
            'listings.*.comps.*.end_time' => ['nullable', 'date'],
            'listings.*.comps.*.final_price' => ['required', 'numeric', 'gt:0'],
            'listings.*.comps.*.bids_count' => ['nullable', 'integer', 'min:0'],
            'listings.*.comps.*.condition_grade' => ['nullable', 'string', 'max:4'],
            'listings.*.comps.*.model_key' => ['nullable', 'string', 'max:500'],
            'listings.*.comps.*.text_scan_grade' => ['nullable', 'string', 'max:16'],
            'listings.*.comps.*.text_scan_checked' => ['nullable', 'boolean'],
            'listings.*.comps.*.text_scan_review_required' => ['nullable', 'boolean'],
            'listings.*.comps.*.text_scan_risk_flags' => ['nullable', 'array'],
            'listings.*.comps.*.text_scan_risk_flags.*' => ['string', 'max:64'],
            'listings.*.comps.*.text_scan_model_name' => ['nullable', 'string', 'max:255'],
            'listings.*.comps.*.text_scan_checked_at' => ['nullable', 'date'],
            'listings.*.comps.*.visual_condition_grade' => ['nullable', 'string', 'max:16'],
            'listings.*.comps.*.visual_condition_checked' => ['nullable', 'boolean'],
            'listings.*.comps.*.visual_review_required' => ['nullable', 'boolean'],
            'listings.*.comps.*.visual_damage_flags' => ['nullable', 'array'],
            'listings.*.comps.*.visual_damage_flags.*' => ['string', 'max:64'],
            'listings.*.comps.*.visual_model_name' => ['nullable', 'string', 'max:255'],
            'listings.*.comps.*.visual_checked_at' => ['nullable', 'date'],
        ]);

        if ($validator->fails()) {
            throw new ValidationException($validator);
        }

        return $validator->validated();
    }

    /** @param array<string, mixed> $payload */
    private function isFinalChunk(array $payload): bool
    {
        if (! isset($payload['chunk']) || ! is_array($payload['chunk'])) {
            return true;
        }

        return (bool) ($payload['chunk']['final'] ?? false);
    }

    /** @return list<string> */
    private function listingFields(): array
    {
        return [
            'source', 'title', 'url', 'model_key', 'condition_grade',
            'text_condition_grade', 'text_scan_grade', 'text_scan_checked',
            'text_scan_review_required', 'text_scan_risk_flags',
            'text_scan_model_name', 'text_scan_checked_at', 'visual_condition_grade',
            'visual_condition_checked', 'visual_review_required',
            'visual_damage_flags', 'visual_model_name', 'visual_checked_at',
            'sale_type', 'end_time', 'current_price', 'sold_median',
            'comp_samples', 'max_safe_buy', 'expected_profit', 'expected_roi',
            'price_gap_to_max_buy', 'effective_min_comps', 'signal',
        ];
    }

    /** @return list<string> */
    private function compFields(): array
    {
        return [
            'source', 'title', 'url', 'sale_type', 'end_time', 'final_price',
            'bids_count', 'condition_grade', 'model_key', 'text_scan_grade',
            'text_scan_checked', 'text_scan_review_required', 'text_scan_risk_flags',
            'text_scan_model_name', 'text_scan_checked_at', 'visual_condition_grade',
            'visual_condition_checked', 'visual_review_required', 'visual_damage_flags',
            'visual_model_name', 'visual_checked_at',
        ];
    }
}
