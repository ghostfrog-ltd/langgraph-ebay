<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\PublishedSoldEvidence;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\ValidationException;
use Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException;

class SoldEvidenceSyncController extends Controller
{
    public function __invoke(Request $request): JsonResponse
    {
        $this->verifySignature($request);
        $payload = $this->validatePayload($request->json()->all());
        $now = now();
        $rows = [];

        foreach ($payload['sold_evidence'] as $soldRow) {
            if (array_key_exists('visual_damage_flags', $soldRow)) {
                $soldRow['visual_damage_flags'] = json_encode(
                    $soldRow['visual_damage_flags'] ?? [],
                    JSON_THROW_ON_ERROR,
                );
            }
            if (array_key_exists('text_scan_risk_flags', $soldRow)) {
                $soldRow['text_scan_risk_flags'] = json_encode(
                    $soldRow['text_scan_risk_flags'] ?? [],
                    JSON_THROW_ON_ERROR,
                );
            }
            $rows[] = [
                'external_id' => $soldRow['external_id'],
                ...Arr::only($soldRow, $this->fields()),
                'synced_at' => $payload['generated_at'],
                'sync_batch_id' => $payload['batch_id'],
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }

        foreach (array_chunk($rows, 1000) as $chunk) {
            PublishedSoldEvidence::query()->upsert(
                $chunk,
                ['external_id'],
                [
                    ...$this->fields(),
                    'synced_at',
                    'sync_batch_id',
                    'updated_at',
                ],
            );
        }

        if (($payload['full_snapshot'] ?? false) && $this->isFinalChunk($payload)) {
            PublishedSoldEvidence::query()
                ->where('sync_batch_id', '!=', $payload['batch_id'])
                ->orWhereNull('sync_batch_id')
                ->delete();
        }

        return response()->json([
            'batch_id' => $payload['batch_id'],
            'sold_evidence' => count($rows),
            'final' => $this->isFinalChunk($payload),
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

    /**
     * @param  array<string, mixed>  $payload
     * @return array<string, mixed>
     */
    private function validatePayload(array $payload): array
    {
        $validator = Validator::make($payload, [
            'schema_version' => ['required', 'integer', 'in:1'],
            'batch_id' => ['required', 'uuid'],
            'generated_at' => ['required', 'date'],
            'full_snapshot' => ['nullable', 'boolean'],
            'chunk' => ['nullable', 'array'],
            'chunk.index' => ['required_with:chunk', 'integer', 'min:1'],
            'chunk.total' => ['required_with:chunk', 'integer', 'min:1'],
            'chunk.final' => ['required_with:chunk', 'boolean'],
            'sold_evidence' => ['required', 'array', 'max:5000'],
            'sold_evidence.*.external_id' => ['required', 'string', 'max:64'],
            'sold_evidence.*.source' => ['required', 'string', 'max:100'],
            'sold_evidence.*.title' => ['required', 'string', 'max:1000'],
            'sold_evidence.*.url' => ['nullable', 'url', 'max:2000'],
            'sold_evidence.*.sale_type' => ['nullable', 'string', 'max:100'],
            'sold_evidence.*.end_time' => ['nullable', 'date'],
            'sold_evidence.*.final_price' => ['required', 'numeric', 'gt:0'],
            'sold_evidence.*.bids_count' => ['nullable', 'integer', 'min:0'],
            'sold_evidence.*.condition_grade' => ['nullable', 'string', 'max:4'],
            'sold_evidence.*.model_key' => ['nullable', 'string', 'max:500'],
            'sold_evidence.*.text_scan_grade' => ['nullable', 'string', 'max:16'],
            'sold_evidence.*.text_scan_checked' => ['nullable', 'boolean'],
            'sold_evidence.*.text_scan_review_required' => ['nullable', 'boolean'],
            'sold_evidence.*.text_scan_risk_flags' => ['nullable', 'array'],
            'sold_evidence.*.text_scan_risk_flags.*' => ['string', 'max:64'],
            'sold_evidence.*.text_scan_model_name' => ['nullable', 'string', 'max:255'],
            'sold_evidence.*.text_scan_checked_at' => ['nullable', 'date'],
            'sold_evidence.*.visual_condition_grade' => ['nullable', 'string', 'max:16'],
            'sold_evidence.*.visual_condition_checked' => ['nullable', 'boolean'],
            'sold_evidence.*.visual_review_required' => ['nullable', 'boolean'],
            'sold_evidence.*.visual_damage_flags' => ['nullable', 'array'],
            'sold_evidence.*.visual_damage_flags.*' => ['string', 'max:64'],
            'sold_evidence.*.visual_model_name' => ['nullable', 'string', 'max:255'],
            'sold_evidence.*.visual_checked_at' => ['nullable', 'date'],
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
    private function fields(): array
    {
        return [
            'source',
            'title',
            'url',
            'sale_type',
            'end_time',
            'final_price',
            'bids_count',
            'condition_grade',
            'model_key',
            'text_scan_grade',
            'text_scan_checked',
            'text_scan_review_required',
            'text_scan_risk_flags',
            'text_scan_model_name',
            'text_scan_checked_at',
            'visual_condition_grade',
            'visual_condition_checked',
            'visual_review_required',
            'visual_damage_flags',
            'visual_model_name',
            'visual_checked_at',
        ];
    }
}
