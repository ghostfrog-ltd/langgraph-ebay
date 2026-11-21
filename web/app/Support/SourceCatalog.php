<?php

namespace App\Support;

use App\Models\PublishedListing;
use Illuminate\Support\Collection;
use Illuminate\Support\Str;

class SourceCatalog
{
    /** @return Collection<int, non-empty-string> */
    public static function sources(): Collection
    {
        return self::orderedSources();
    }

    /** @return Collection<string, positive-int> */
    public static function visibleSourceCounts(): Collection
    {
        /** @var Collection<non-empty-string, int|string> $counts */
        $counts = PublishedListing::query()
            ->selectRaw('source, count(*) as listing_count')
            ->where('max_safe_buy', '>=', 0)
            ->whereNotIn('source', self::hiddenSources())
            ->where(function ($query): void {
                $query
                    ->where('source', 'motomine')
                    ->orWhere('comp_samples', '>', 0);
            })
            ->groupBy('source')
            ->pluck('listing_count', 'source');

        $visibleCounts = [];

        foreach (self::orderedSources() as $source) {
            $count = (int) $counts->get($source, 0);

            if ($count > 0) {
                $visibleCounts[$source] = $count;
            }
        }

        return collect($visibleCounts);
    }

    /** @return Collection<int, non-empty-string> */
    private static function orderedSources(): Collection
    {
        $configuredSources = config('services.ghostfrog.sources', []);
        $sources = [];

        if (is_array($configuredSources)) {
            foreach ($configuredSources as $key => $source) {
                $value = is_int($key) ? $source : $key;

                if (is_string($value) && $value !== '') {
                    $sources[] = $value;
                }
            }
        }

        PublishedListing::query()
            ->select('source')
            ->distinct()
            ->orderBy('source')
            ->pluck('source')
            ->each(function (mixed $source) use (&$sources): void {
                if (is_string($source) && $source !== '') {
                    $sources[] = $source;
                }
            });

        return collect($sources)
            ->unique()
            ->reject(fn (string $source): bool => self::isHidden($source))
            ->values();
    }

    public static function isHidden(string $source): bool
    {
        return self::hiddenSources()->contains($source);
    }

    /** @return Collection<int, non-empty-string> */
    public static function hiddenSources(): Collection
    {
        $hiddenSources = config('services.ghostfrog.hidden_sources', []);

        if (! is_array($hiddenSources)) {
            return collect();
        }

        return collect($hiddenSources)
            ->filter(fn (mixed $source): bool => is_string($source) && $source !== '')
            ->values();
    }

    public static function requiresComps(string $source): bool
    {
        return $source !== 'motomine';
    }

    public static function defaultProfitGate(): int
    {
        return 50;
    }

    public static function profitGate(string $source): int
    {
        $gates = config('services.ghostfrog.source_profit_gates', []);

        if (is_array($gates) && isset($gates[$source])) {
            return max(0, (int) $gates[$source]);
        }

        return self::defaultProfitGate();
    }

    /** @return array<string, int> */
    public static function sourceProfitGates(): array
    {
        $gates = config('services.ghostfrog.source_profit_gates', []);

        if (! is_array($gates)) {
            return [];
        }

        $normalised = [];

        foreach ($gates as $source => $gate) {
            if (is_string($source) && $source !== '') {
                $normalised[$source] = max(0, (int) $gate);
            }
        }

        return $normalised;
    }

    public static function label(string $source): string
    {
        $labels = config('services.ghostfrog.source_labels', []);

        if (is_array($labels) && isset($labels[$source]) && $labels[$source] !== '') {
            return (string) $labels[$source];
        }

        return Str::of($source)
            ->after('ebay-')
            ->replace('-', ' ')
            ->title()
            ->replace('Cpus', 'CPUs')
            ->replace('Pc', 'PC')
            ->replace('Msx125', 'MSX125')
            ->replace('Nc750', 'NC750')
            ->toString();
    }
}
