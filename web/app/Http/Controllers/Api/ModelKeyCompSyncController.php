<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\PublishedModelKeyComp;
use App\Models\PublishedModelKeyCompRow;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\ValidationException;
use Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException;

class ModelKeyCompSyncController extends Controller
{
    public function __invoke(Request $request): JsonResponse
    {
        $this->verifySignature($request);
        $payload = $this->validatePayload($request->json()->all());
        $rows = [];
        $compRows = [];
        $now = now();

        $storedCount = DB::transaction(function () use ($payload, $now, &$rows, &$compRows): int {
            $summaryKeys = [];
            foreach ($payload['model_key_comps'] as $comp) {
                $summaryKeys[] = [
                    'model_key' => $comp['model_key'],
                    'condition_grade' => $comp['condition_grade'],
                ];
                $rows[] = [
                    ...Arr::only($comp, $this->fields()),
                    'synced_at' => $payload['generated_at'],
                    'created_at' => $now,
                    'updated_at' => $now,
                ];
            }

            foreach (array_chunk($rows, 500) as $chunk) {
                PublishedModelKeyComp::query()->upsert(
                    $chunk,
                    ['model_key', 'condition_grade'],
                    [
                        ...$this->fields(),
                        'synced_at',
                        'updated_at',
                    ],
                );
            }

            $summaries = PublishedModelKeyComp::query()
                ->where(function ($query) use ($summaryKeys): void {
                    foreach ($summaryKeys as $index => $key) {
                        $method = $index === 0 ? 'where' : 'orWhere';
                        $query->{$method}(function ($query) use ($key): void {
                            $query
                                ->where('model_key', $key['model_key'])
                                ->where('condition_grade', $key['condition_grade']);
                        });
                    }
                })
                ->get()
                ->keyBy(fn (PublishedModelKeyComp $summary): string => $summary->model_key.'|'.$summary->condition_grade);

            $summaryIds = $summaries->pluck('id')->all();
            if ($summaryIds !== []) {
                PublishedModelKeyCompRow::query()
                    ->whereIn('published_model_key_comp_id', $summaryIds)
                    ->delete();
            }

            foreach ($payload['model_key_comps'] as $summary) {
                $summaryModel = $summaries->get($summary['model_key'].'|'.$summary['condition_grade']);
                if (! $summaryModel) {
                    continue;
                }

                foreach ($summary['comps'] ?? [] as $compRow) {
                    if (array_key_exists('visual_damage_flags', $compRow)) {
                        $compRow['visual_damage_flags'] = json_encode(
                            $compRow['visual_damage_flags'] ?? [],
                            JSON_THROW_ON_ERROR,
                        );
                    }
                    if (array_key_exists('text_scan_risk_flags', $compRow)) {
                        $compRow['text_scan_risk_flags'] = json_encode(
                            $compRow['text_scan_risk_flags'] ?? [],
                            JSON_THROW_ON_ERROR,
                        );
                    }
                    $compRows[] = [
                        'published_model_key_comp_id' => $summaryModel->id,
                        'comp_external_id' => $compRow['external_id'],
                        ...Arr::only($compRow, $this->compRowFields()),
                        'created_at' => $now,
                        'updated_at' => $now,
                    ];
                }
            }

            foreach (array_chunk($compRows, 1000) as $chunk) {
                DB::table('published_model_key_comp_rows')->insert($chunk);
            }

            return count($rows);
        });

        return response()->json([
            'batch_id' => $payload['batch_id'],
            'model_key_comps' => $storedCount,
            'comp_rows' => count($compRows),
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
            'model_key_comps' => ['required', 'array', 'min:1', 'max:500'],
            'model_key_comps.*.source' => ['required', 'string', 'max:100'],
            'model_key_comps.*.model_key' => ['required', 'string', 'max:500'],
            'model_key_comps.*.condition_grade' => ['required', 'string', 'max:4'],
            'model_key_comps.*.samples' => ['required', 'integer', 'min:1'],
            'model_key_comps.*.median_final_price' => ['required', 'numeric', 'gt:0'],
            'model_key_comps.*.computed_at' => ['nullable', 'date'],
            'model_key_comps.*.comps' => ['present', 'array', 'max:500'],
            'model_key_comps.*.comps.*.external_id' => ['required', 'string', 'max:64'],
            'model_key_comps.*.comps.*.source' => ['nullable', 'string', 'max:100'],
            'model_key_comps.*.comps.*.title' => ['required', 'string', 'max:1000'],
            'model_key_comps.*.comps.*.url' => ['nullable', 'url', 'max:2000'],
            'model_key_comps.*.comps.*.sale_type' => ['nullable', 'string', 'max:100'],
            'model_key_comps.*.comps.*.end_time' => ['nullable', 'date'],
            'model_key_comps.*.comps.*.final_price' => ['required', 'numeric', 'gt:0'],
            'model_key_comps.*.comps.*.bids_count' => ['nullable', 'integer', 'min:0'],
            'model_key_comps.*.comps.*.text_scan_grade' => ['nullable', 'string', 'max:16'],
            'model_key_comps.*.comps.*.text_scan_checked' => ['nullable', 'boolean'],
            'model_key_comps.*.comps.*.text_scan_review_required' => ['nullable', 'boolean'],
            'model_key_comps.*.comps.*.text_scan_risk_flags' => ['nullable', 'array'],
            'model_key_comps.*.comps.*.text_scan_risk_flags.*' => ['string', 'max:64'],
            'model_key_comps.*.comps.*.text_scan_model_name' => ['nullable', 'string', 'max:255'],
            'model_key_comps.*.comps.*.text_scan_checked_at' => ['nullable', 'date'],
            'model_key_comps.*.comps.*.visual_condition_grade' => ['nullable', 'string', 'max:16'],
            'model_key_comps.*.comps.*.visual_condition_checked' => ['nullable', 'boolean'],
            'model_key_comps.*.comps.*.visual_review_required' => ['nullable', 'boolean'],
            'model_key_comps.*.comps.*.visual_damage_flags' => ['nullable', 'array'],
            'model_key_comps.*.comps.*.visual_damage_flags.*' => ['string', 'max:64'],
            'model_key_comps.*.comps.*.visual_model_name' => ['nullable', 'string', 'max:255'],
            'model_key_comps.*.comps.*.visual_checked_at' => ['nullable', 'date'],
        ]);

        if ($validator->fails()) {
            throw new ValidationException($validator);
        }

        return $validator->validated();
    }

    /** @return list<string> */
    private function fields(): array
    {
        return [
            'source',
            'model_key',
            'condition_grade',
            'samples',
            'median_final_price',
            'computed_at',
        ];
    }

    /** @return list<string> */
    private function compRowFields(): array
    {
        return [
            'source',
            'title',
            'url',
            'sale_type',
            'end_time',
            'final_price',
            'bids_count',
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
