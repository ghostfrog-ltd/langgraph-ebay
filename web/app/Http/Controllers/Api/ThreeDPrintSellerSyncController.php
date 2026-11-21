<?php

namespace App\Http\Controllers\Api;

use App\Http\Controllers\Controller;
use App\Models\ThreeDPrintSeller;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\ValidationException;
use Symfony\Component\HttpKernel\Exception\AccessDeniedHttpException;

class ThreeDPrintSellerSyncController extends Controller
{
    public function __invoke(Request $request): JsonResponse
    {
        $this->verifySignature($request);
        $payload = $this->validatePayload($request->json()->all());
        $now = now();
        $rows = [];

        foreach ($payload['sellers'] as $seller) {
            $rows[] = [
                ...Arr::only($seller, $this->fields()),
                'matched_queries' => json_encode($seller['matched_queries'], JSON_THROW_ON_ERROR),
                'example_titles' => json_encode($seller['example_titles'], JSON_THROW_ON_ERROR),
                'marketplace' => $payload['marketplace'],
                'sync_batch_id' => $payload['batch_id'],
                'snapshot_at' => $payload['generated_at'],
                'created_at' => $now,
                'updated_at' => $now,
            ];
        }

        foreach (array_chunk($rows, 500) as $chunk) {
            ThreeDPrintSeller::query()->upsert(
                $chunk,
                ['seller_username'],
                [
                    ...$this->fields(),
                    'marketplace',
                    'sync_batch_id',
                    'snapshot_at',
                    'updated_at',
                ],
            );
        }

        ThreeDPrintSeller::query()
            ->where('sync_batch_id', '!=', $payload['batch_id'])
            ->orWhereNull('sync_batch_id')
            ->delete();

        return response()->json([
            'batch_id' => $payload['batch_id'],
            'sellers' => count($rows),
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
            'marketplace' => ['required', 'string', 'max:32'],
            'sellers' => ['required', 'array', 'max:1000'],
            'sellers.*.rank' => ['required', 'integer', 'min:1'],
            'sellers.*.seller_username' => ['required', 'string', 'max:255'],
            'sellers.*.item_count' => ['required', 'integer', 'min:0'],
            'sellers.*.active_value' => ['required', 'numeric', 'min:0'],
            'sellers.*.max_price' => ['required', 'numeric', 'min:0'],
            'sellers.*.feedback_score' => ['required', 'integer', 'min:0'],
            'sellers.*.feedback_percentage' => ['required', 'numeric', 'min:0', 'max:100'],
            'sellers.*.matched_queries' => ['required', 'array', 'max:20'],
            'sellers.*.matched_queries.*' => ['required', 'string', 'max:120'],
            'sellers.*.example_titles' => ['required', 'array', 'max:5'],
            'sellers.*.example_titles.*' => ['required', 'string', 'max:1000'],
            'sellers.*.example_url' => ['nullable', 'url', 'max:2000'],
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
            'rank',
            'seller_username',
            'item_count',
            'active_value',
            'max_price',
            'feedback_score',
            'feedback_percentage',
            'matched_queries',
            'example_titles',
            'example_url',
        ];
    }
}
