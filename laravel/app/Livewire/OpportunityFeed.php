<?php

namespace App\Livewire;

use App\Models\EbayListing;
use Illuminate\Contracts\View\View;
use Livewire\Attributes\On;
use Livewire\Component;

class OpportunityFeed extends Component
{
    #[On('opportunity-dismissed')]
    public function refreshFeed(): void
    {
        // Re-render after a child card dismisses a listing.
    }

    public function render(): View
    {
        $allListingsCount = EbayListing::query()->count();
        $recentlySyncedCount = EbayListing::query()
            ->whereNotNull('last_synced_at')
            ->where('last_synced_at', '>=', now()->subHour())
            ->count();

        $opportunities = EbayListing::query()
            ->hotOpportunities()
            ->orderByRaw('((estimated_market_value - current_price) * 100.0) / current_price DESC')
            ->get();

        return view('livewire.opportunity-feed', [
            'allListingsCount' => $allListingsCount,
            'opportunities' => $opportunities,
            'recentlySyncedCount' => $recentlySyncedCount,
        ]);
    }
}
