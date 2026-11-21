<?php

namespace App\Livewire\Listings;

use App\Models\PublishedListing;
use App\Models\SyncRun;
use App\Support\ListingSearch;
use App\Support\SourceCatalog;
use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Attributes\Url;
use Livewire\Component;
use Livewire\WithPagination;

#[Title('Live listings')]
class Index extends Component
{
    use WithPagination;

    #[Url]
    public int $profit = 50;

    #[Url]
    public string $search = '';

    #[Url]
    public string $source = '';

    #[Url]
    public string $saleType = 'bin';

    #[Url]
    public string $grade = '';

    #[Url]
    public string $visual = 'all';

    #[Url]
    public string $textScan = 'all';

    #[Url]
    public string $sort = 'priority';

    public function mount(): void
    {
        $this->saleType = $this->normaliseSaleType($this->saleType);
        $this->grade = $this->normaliseGrade($this->grade);
        $this->visual = $this->normaliseVisual($this->visual);
        $this->textScan = $this->normaliseTextScan($this->textScan);
        $this->sort = $this->normaliseSort($this->sort);
    }

    public function updated(string $property): void
    {
        if ($property === 'saleType') {
            $this->saleType = $this->normaliseSaleType($this->saleType);
        }

        if ($property === 'grade') {
            $this->grade = $this->normaliseGrade($this->grade);
        }

        if ($property === 'visual') {
            $this->visual = $this->normaliseVisual($this->visual);
        }

        if ($property === 'textScan') {
            $this->textScan = $this->normaliseTextScan($this->textScan);
        }

        if ($property === 'sort') {
            $this->sort = $this->normaliseSort($this->sort);
        }

        if (in_array($property, ['profit', 'search', 'source', 'saleType', 'grade', 'visual', 'textScan', 'sort'], true)) {
            $this->resetPage();
        }
    }

    public function chooseProfit(int $profit): void
    {
        $this->profit = in_array($profit, [25, 50], true) ? $profit : 50;
        $this->resetPage();
    }

    public function chooseSaleType(string $saleType): void
    {
        $this->saleType = $this->normaliseSaleType($saleType);
        $this->resetPage();
    }

    public function chooseVisual(string $visual): void
    {
        $this->visual = $this->normaliseVisual($visual);
        $this->resetPage();
    }

    public function chooseTextScan(string $textScan): void
    {
        $this->textScan = $this->normaliseTextScan($textScan);
        $this->resetPage();
    }

    public function chooseSort(string $sort): void
    {
        $this->sort = $this->normaliseSort($sort);
        $this->resetPage();
    }

    public function clearFilters(): void
    {
        $this->profit = 50;
        $this->search = '';
        $this->source = '';
        $this->saleType = 'bin';
        $this->grade = '';
        $this->visual = 'all';
        $this->textScan = 'all';
        $this->sort = 'priority';
        $this->resetPage();
    }

    public function render(): View
    {
        $query = $this->filteredQuery();
        $minimum = max($this->profit, 0);
        $nearMinimum = max($minimum - 10, 0);

        /** @var LengthAwarePaginator<int, PublishedListing> $potentials */
        $potentials = $this->prioritisedQuery(
            $this->profitBandQuery(clone $query, $minimum)
        )->paginate(12);

        $nearMisses = (clone $query)
            ->where('expected_profit', '>=', $nearMinimum)
            ->where('expected_profit', '<', $minimum)
            ->orderByRaw('end_time IS NULL')
            ->orderBy('end_time')
            ->orderByDesc('expected_profit')
            ->limit(10)
            ->get();

        return view('livewire.listings.index', [
            'potentials' => $potentials,
            'nearMisses' => $nearMisses,
            'sources' => SourceCatalog::visibleSourceCounts()->keys()->values(),
            'latestSync' => SyncRun::query()->latest('received_at')->first(),
            'matchingCount' => (clone $query)->count(),
            'potentialCount' => $this->profitBandQuery(clone $query, $minimum)->count(),
            'totalCount' => PublishedListing::query()->count(),
            'grades' => $this->availableGrades(),
        ]);
    }

    /**
     * @param  Builder<PublishedListing>  $query
     * @return Builder<PublishedListing>
     */
    private function prioritisedQuery(Builder $query): Builder
    {
        if ($this->sort === 'ending') {
            return $query
                ->orderByRaw('end_time IS NULL')
                ->orderBy('end_time')
                ->orderByDesc('expected_profit');
        }

        if ($this->sort === 'profit') {
            return $query
                ->orderByDesc('expected_profit')
                ->orderByDesc('expected_roi')
                ->orderByDesc('comp_samples')
                ->orderByRaw('end_time IS NULL')
                ->orderBy('end_time');
        }

        return $query
            ->orderByDesc('visual_condition_checked')
            ->orderBy('visual_review_required')
            ->orderByDesc('price_gap_to_max_buy')
            ->orderByDesc('expected_profit')
            ->orderByDesc('expected_roi')
            ->orderByDesc('comp_samples')
            ->orderByRaw('end_time IS NULL')
            ->orderBy('end_time');
    }

    /**
     * @param  Builder<PublishedListing>  $query
     * @return Builder<PublishedListing>
     */
    private function profitBandQuery(Builder $query, int $minimum): Builder
    {
        if ($minimum === SourceCatalog::defaultProfitGate()) {
            return $this->defaultGateQuery($query);
        }

        $query->where('expected_profit', '>=', $minimum);

        if ($minimum === 25) {
            $query->where('expected_profit', '<', 50);
        }

        return $query;
    }

    /**
     * @param  Builder<PublishedListing>  $query
     * @return Builder<PublishedListing>
     */
    private function defaultGateQuery(Builder $query): Builder
    {
        $sourceGates = SourceCatalog::sourceProfitGates();
        $sourcesWithCustomGates = array_keys($sourceGates);
        $defaultGate = SourceCatalog::defaultProfitGate();

        return $query->where(function (Builder $query) use ($sourceGates, $sourcesWithCustomGates, $defaultGate): void {
            foreach ($sourceGates as $source => $gate) {
                $query->orWhere(function (Builder $query) use ($source, $gate): void {
                    $query
                        ->where('source', $source)
                        ->where('expected_profit', '>=', $gate);
                });
            }

            $query->orWhere(function (Builder $query) use ($sourcesWithCustomGates, $defaultGate): void {
                if ($sourcesWithCustomGates !== []) {
                    $query->whereNotIn('source', $sourcesWithCustomGates);
                }

                $query->where('expected_profit', '>=', $defaultGate);
            });
        });
    }

    /** @return Builder<PublishedListing> */
    private function filteredQuery(): Builder
    {
        $query = PublishedListing::query()
            ->whereNotIn('source', SourceCatalog::hiddenSources())
            ->where('comp_samples', '>=', $this->buySignalMinComps())
            ->when($this->source !== '', fn (Builder $query) => $query->where('source', $this->source))
            ->when($this->saleType !== 'both', fn (Builder $query) => $query->where('sale_type', $this->saleType))
            ->when($this->grade !== '', fn (Builder $query) => $query->where('condition_grade', $this->grade))
            ->when($this->visual === 'checked', fn (Builder $query) => $query->where('visual_condition_checked', true))
            ->when($this->visual === 'unchecked', fn (Builder $query) => $query->where('visual_condition_checked', false))
            ->when($this->textScan === 'checked', fn (Builder $query) => $query->where('text_scan_checked', true))
            ->when($this->textScan === 'unchecked', fn (Builder $query) => $query->where('text_scan_checked', false));

        return ListingSearch::apply($query, $this->search);
    }

    /** @return list<string> */
    private function availableGrades(): array
    {
        $query = PublishedListing::query()
            ->whereNotIn('source', SourceCatalog::hiddenSources())
            ->where('comp_samples', '>=', $this->buySignalMinComps())
            ->when($this->source !== '', fn (Builder $query) => $query->where('source', $this->source))
            ->when($this->saleType !== 'both', fn (Builder $query) => $query->where('sale_type', $this->saleType))
            ->when($this->visual === 'checked', fn (Builder $query) => $query->where('visual_condition_checked', true))
            ->when($this->visual === 'unchecked', fn (Builder $query) => $query->where('visual_condition_checked', false))
            ->when($this->textScan === 'checked', fn (Builder $query) => $query->where('text_scan_checked', true))
            ->when($this->textScan === 'unchecked', fn (Builder $query) => $query->where('text_scan_checked', false))
            ->whereNotNull('condition_grade');

        ListingSearch::apply($query, $this->search);

        return $query
            ->distinct()
            ->orderBy('condition_grade')
            ->pluck('condition_grade')
            ->map(fn (string $grade): string => $this->normaliseGrade($grade))
            ->filter()
            ->unique()
            ->values()
            ->all();
    }

    private function buySignalMinComps(): int
    {
        return max(1, (int) config('services.ghostfrog.buy_signal_min_comps', 100));
    }

    private function normaliseSaleType(string $saleType): string
    {
        return in_array($saleType, ['both', 'bin', 'auction'], true) ? $saleType : 'bin';
    }

    private function normaliseGrade(string $grade): string
    {
        $grade = strtoupper(trim($grade));

        return strlen($grade) <= 4 ? $grade : '';
    }

    private function normaliseVisual(string $visual): string
    {
        return in_array($visual, ['checked', 'unchecked'], true) ? $visual : 'all';
    }

    private function normaliseTextScan(string $textScan): string
    {
        return in_array($textScan, ['checked', 'unchecked'], true) ? $textScan : 'all';
    }

    private function normaliseSort(string $sort): string
    {
        return in_array($sort, ['ending', 'profit'], true) ? $sort : 'priority';
    }
}
