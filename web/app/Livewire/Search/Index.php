<?php

namespace App\Livewire\Search;

use App\Models\ListingFavourite;
use App\Models\PublishedListing;
use App\Models\PublishedModelKeyComp;
use App\Models\PublishedSoldEvidence;
use App\Support\ListingSearch;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Collection;
use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Attributes\Url;
use Livewire\Component;

#[Title('Search evidence desk')]
class Index extends Component
{
    #[Url]
    public string $search = '';

    public function clearSearch(): void
    {
        $this->search = '';
    }

    public function render(): View
    {
        $hasSearch = trim($this->search) !== '';

        /** @var Collection<int, PublishedListing> $listings */
        $listings = collect();
        /** @var Collection<int, PublishedSoldEvidence> $comps */
        $comps = collect();
        /** @var Collection<int, ListingFavourite> $favourites */
        $favourites = collect();
        /** @var Collection<int, PublishedModelKeyComp> $modelKeyComps */
        $modelKeyComps = collect();

        $listingCount = 0;
        $compCount = 0;
        $favouriteCount = 0;
        $modelKeyCompCount = 0;

        if ($hasSearch) {
            $listingQuery = $this->listingQuery();
            $compQuery = $this->compQuery();
            $favouriteQuery = $this->favouriteQuery();
            $modelKeyCompQuery = $this->modelKeyCompQuery();

            $listingCount = (clone $listingQuery)->count();
            $compCount = (clone $compQuery)->count();
            $favouriteCount = (clone $favouriteQuery)->count();
            $modelKeyCompCount = (clone $modelKeyCompQuery)->count();

            /** @var Collection<int, PublishedListing> $listings */
            $listings = $listingQuery
                ->orderByRaw('end_time IS NULL')
                ->orderBy('end_time')
                ->orderByDesc('expected_profit')
                ->limit(24)
                ->get();

            /** @var Collection<int, PublishedSoldEvidence> $comps */
            $comps = $compQuery
                ->orderByDesc('visual_condition_checked')
                ->orderBy('visual_review_required')
                ->orderByDesc('end_time')
                ->orderByDesc('final_price')
                ->limit(24)
                ->get();

            /** @var Collection<int, ListingFavourite> $favourites */
            $favourites = $favouriteQuery
                ->with('currentListing')
                ->orderByDesc('favourited_at')
                ->limit(12)
                ->get();

            /** @var Collection<int, PublishedModelKeyComp> $modelKeyComps */
            $modelKeyComps = $modelKeyCompQuery
                ->orderByDesc('samples')
                ->orderBy('condition_grade')
                ->limit(24)
                ->get();
        }

        return view('livewire.search.index', [
            'hasSearch' => $hasSearch,
            'listings' => $listings,
            'listingCount' => $listingCount,
            'comps' => $comps,
            'compCount' => $compCount,
            'favourites' => $favourites,
            'favouriteCount' => $favouriteCount,
            'modelKeyComps' => $modelKeyComps,
            'modelKeyCompCount' => $modelKeyCompCount,
        ]);
    }

    /** @return Builder<PublishedListing> */
    private function listingQuery(): Builder
    {
        return ListingSearch::apply(PublishedListing::query(), $this->search);
    }

    /** @return Builder<PublishedSoldEvidence> */
    private function compQuery(): Builder
    {
        return ListingSearch::applyToColumns(PublishedSoldEvidence::query(), $this->search, [
            'title',
            'model_key',
            'source',
            'external_id',
        ]);
    }

    /** @return Builder<ListingFavourite> */
    private function favouriteQuery(): Builder
    {
        return ListingSearch::applyToColumns(
            ListingFavourite::query()->where('user_id', auth()->id()),
            $this->search,
            [
                'title',
                'model_key',
                'source',
                'external_id',
            ],
        );
    }

    /** @return Builder<PublishedModelKeyComp> */
    private function modelKeyCompQuery(): Builder
    {
        return ListingSearch::applyToColumns(PublishedModelKeyComp::query(), $this->search, [
            'source',
            'model_key',
        ]);
    }
}
