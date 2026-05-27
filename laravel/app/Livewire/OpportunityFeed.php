<?php

namespace App\Livewire;

use App\Models\EbayListing;
use Illuminate\Contracts\View\View;
use Livewire\Attributes\On;
use Livewire\Component;

class OpportunityFeed extends Component
{
    public int $visibleCount = 3;

    #[On('opportunity-dismissed')]
    public function refreshFeed(): void
    {
        // Re-render after a child card dismisses a listing.
    }

    public function loadMore(): void
    {
        $this->visibleCount += 5;
    }

    public function render(): View
    {
        $allListingsCount = EbayListing::query()->count();
        $recentlySyncedCount = EbayListing::query()
            ->whereNotNull('last_synced_at')
            ->where('last_synced_at', '>=', now()->subHour())
            ->count();

        $opportunityQuery = EbayListing::query()
            ->hotOpportunities()
            ->orderByRaw('((estimated_market_value - current_price) * 100.0) / current_price DESC');

        $opportunityCount = (clone $opportunityQuery)->count();
        $bestRoi = (clone $opportunityQuery)
            ->reorder()
            ->selectRaw('MAX(((estimated_market_value - current_price) * 100.0) / current_price) as max_roi')
            ->value('max_roi');

        $opportunities = $opportunityQuery
            ->limit($this->visibleCount)
            ->get();

        return view('livewire.opportunity-feed', [
            'allListingsCount' => $allListingsCount,
            'bestRoi' => $bestRoi,
            'hasMoreOpportunities' => $opportunityCount > $opportunities->count(),
            'opportunityCount' => $opportunityCount,
            'opportunities' => $opportunities,
            'recentlySyncedCount' => $recentlySyncedCount,
        ]);
    }
}
