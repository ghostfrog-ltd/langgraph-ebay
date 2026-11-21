<?php

namespace App\Livewire\Listings;

use App\Models\PublishedListing;
use App\Support\ListingSearch;
use App\Support\SourceCatalog;
use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Attributes\Url;
use Livewire\Component;
use Livewire\WithPagination;

#[Title('Source listings')]
class SourceIndex extends Component
{
    use WithPagination;

    public string $source;

    #[Url]
    public string $search = '';

    #[Url]
    public string $gate = 'clears';

    #[Url]
    public string $saleType = 'bin';

    #[Url]
    public string $grade = '';

    #[Url]
    public string $visual = 'all';

    #[Url]
    public string $textScan = 'all';

    public function mount(string $source): void
    {
        $this->source = $source;
        $this->gate = $this->normaliseGate($this->gate);
        $this->saleType = $this->normaliseSaleType($this->saleType);
        $this->grade = $this->normaliseGrade($this->grade);
        $this->visual = $this->normaliseVisual($this->visual);
        $this->textScan = $this->normaliseTextScan($this->textScan);
    }

    public function updated(string $property): void
    {
        if ($property === 'gate') {
            $this->gate = $this->normaliseGate($this->gate);
        }

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

        if (in_array($property, ['search', 'gate', 'saleType', 'grade', 'visual', 'textScan'], true)) {
            $this->resetPage();
        }
    }

    public function chooseGate(string $gate): void
    {
        $this->gate = $this->normaliseGate($gate);
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

    public function clearFilters(): void
    {
        $this->search = '';
        $this->gate = 'clears';
        $this->saleType = 'bin';
        $this->grade = '';
        $this->visual = 'all';
        $this->textScan = 'all';
        $this->resetPage();
    }

    public function render(): View
    {
        $query = $this->filteredQuery();

        /** @var LengthAwarePaginator<int, PublishedListing> $listings */
        $listings = (clone $query)
            ->orderByRaw('end_time IS NULL')
            ->orderBy('end_time')
            ->orderBy('external_id')
            ->paginate(24);

        return view('livewire.listings.source-index', [
            'listings' => $listings,
            'listingCount' => (clone $query)->count(),
            'source' => $this->source,
            'gate' => $this->gate,
            'profitGate' => SourceCatalog::profitGate($this->source),
            'sourceLabel' => SourceCatalog::label($this->source),
            'grades' => $this->availableGrades(),
        ]);
    }

    /** @return \Illuminate\Database\Eloquent\Builder<PublishedListing> */
    private function filteredQuery(): \Illuminate\Database\Eloquent\Builder
    {
        $query = $this->baseQuery()
            ->when($this->grade !== '', fn ($query) => $query->where('condition_grade', $this->grade))
            ->when($this->visual === 'checked', fn ($query) => $query->where('visual_condition_checked', true))
            ->when($this->visual === 'unchecked', fn ($query) => $query->where('visual_condition_checked', false))
            ->when($this->textScan === 'checked', fn ($query) => $query->where('text_scan_checked', true))
            ->when($this->textScan === 'unchecked', fn ($query) => $query->where('text_scan_checked', false));

        ListingSearch::apply($query, $this->search);

        return $query;
    }

    /** @return \Illuminate\Database\Eloquent\Builder<PublishedListing> */
    private function baseQuery(): \Illuminate\Database\Eloquent\Builder
    {
        return PublishedListing::query()
            ->where('source', $this->source)
            ->where('max_safe_buy', '>=', 0)
            ->when(SourceCatalog::isHidden($this->source), fn ($query) => $query->whereRaw('1 = 0'))
            ->when(SourceCatalog::requiresComps($this->source), fn ($query) => $query->where('comp_samples', '>', 0))
            ->when($this->gate === 'clears', fn ($query) => $query->where('expected_profit', '>=', SourceCatalog::profitGate($this->source)))
            ->when($this->saleType !== 'both', fn ($query) => $query->where('sale_type', $this->saleType));
    }

    /** @return list<string> */
    private function availableGrades(): array
    {
        $query = $this->baseQuery()
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

    private function normaliseGate(string $gate): string
    {
        return $gate === 'all' ? 'all' : 'clears';
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
}
