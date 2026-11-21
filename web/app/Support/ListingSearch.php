<?php

namespace App\Support;

use App\Models\PublishedListing;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;

class ListingSearch
{
    /**
     * @param  Builder<PublishedListing>  $query
     * @return Builder<PublishedListing>
     */
    public static function apply(Builder $query, string $search): Builder
    {
        return self::applyToColumns($query, $search, [
            'title',
            'model_key',
            'source',
            'external_id',
        ]);
    }

    /**
     * @template TModel of Model
     *
     * @param  Builder<TModel>  $query
     * @param  list<non-empty-string>  $columns
     * @return Builder<TModel>
     */
    public static function applyToColumns(Builder $query, string $search, array $columns): Builder
    {
        $tokens = self::tokens($search);

        foreach ($tokens as $token) {
            $query->where(function (Builder $query) use ($columns, $token): void {
                $searchableColumns = array_values(array_filter(
                    array_map(fn (string $column): ?array => self::searchClause($column, $token), $columns),
                ));

                foreach ($searchableColumns as $index => [$clause, $term]) {
                    if ($index === 0) {
                        $query->whereRaw($clause, [$term]);

                        continue;
                    }

                    $query->orWhereRaw($clause, [$term]);
                }
            });
        }

        return $query;
    }

    /** @return list<string> */
    private static function tokens(string $search): array
    {
        $parts = preg_split('/\s+/', mb_strtolower(trim($search))) ?: [];

        return array_values(array_filter($parts, fn (string $part): bool => $part !== ''));
    }

    private static function normalizeModelKeyToken(string $value): string
    {
        return preg_replace('/[^a-z0-9]+/', '', $value) ?? '';
    }

    private static function escapeLike(string $value): string
    {
        return str_replace(['\\', '%', '_'], ['\\\\', '\\%', '\\_'], $value);
    }

    /**
     * @return array{literal-string, string}|null
     */
    private static function searchClause(string $column, string $token): ?array
    {
        if ($column === 'model_key') {
            $normalized = self::normalizeModelKeyToken($token);
            if ($normalized !== '') {
                return [
                    "LOWER(REPLACE(REPLACE(REPLACE(REPLACE(CAST(model_key AS TEXT), '-', ''), '_', ''), '/', ''), ' ', '')) LIKE ? ESCAPE '\\'",
                    '%'.self::escapeLike($normalized).'%',
                ];
            }
        }

        $term = '%'.self::escapeLike($token).'%';

        $clause = match ($column) {
            'comp_external_id' => "LOWER(CAST(comp_external_id AS TEXT)) LIKE ? ESCAPE '\\'",
            'external_id' => "LOWER(CAST(external_id AS TEXT)) LIKE ? ESCAPE '\\'",
            'listing_external_id' => "LOWER(CAST(listing_external_id AS TEXT)) LIKE ? ESCAPE '\\'",
            'model_key' => "LOWER(CAST(model_key AS TEXT)) LIKE ? ESCAPE '\\'",
            'source' => "LOWER(CAST(source AS TEXT)) LIKE ? ESCAPE '\\'",
            'title' => "LOWER(CAST(title AS TEXT)) LIKE ? ESCAPE '\\'",
            default => null,
        };

        if ($clause === null) {
            return null;
        }

        return [$clause, $term];
    }
}
