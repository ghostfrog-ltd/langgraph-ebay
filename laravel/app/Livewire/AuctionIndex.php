<?php

namespace App\Livewire;

use App\Models\EbayListing;
use Illuminate\Contracts\View\View;
use Livewire\Attributes\Computed;
use Livewire\Component;
use Livewire\WithPagination;

class AuctionIndex extends Component
{
    use WithPagination;

    public int $perPage = 250;

    public array $selectedIds = [];

    public ?int $selectedListingId = null;

    #[Computed]
    public function selectedListing(): ?EbayListing
    {
        if (! $this->selectedListingId) {
            return null;
        }

        return EbayListing::query()->find($this->selectedListingId);
    }

    public function openListing(int $listingId): void
    {
        $this->selectedListingId = $listingId;
    }

    public function closeListing(): void
    {
        $this->selectedListingId = null;
    }

    public function bulkDismiss(): void
    {
        if ($this->selectedIds === []) {
            return;
        }

        EbayListing::query()
            ->whereIn('id', $this->selectedIds)
            ->update(['pipeline_state' => 'dismissed']);

        $this->selectedIds = [];
        $this->dispatch('pipeline-status-changed');
        $this->dispatch('opportunity-dismissed');
    }

    public function bulkReanalyze(): void
    {
        if ($this->selectedIds === []) {
            return;
        }

        EbayListing::query()
            ->whereIn('id', $this->selectedIds)
            ->update(['pipeline_state' => 'analyzing']);

        $this->selectedIds = [];
        $this->dispatch('pipeline-status-changed');
        $this->dispatch('opportunity-dismissed');
    }

    public function render(): View
    {
        return view('livewire.auction-index', [
            'listings' => EbayListing::query()
                ->latest()
                ->paginate($this->perPage),
            'selectedListing' => $this->selectedListing,
            'sourcePlatforms' => EbayListing::query()
                ->whereNotNull('source_platform')
                ->distinct()
                ->orderBy('source_platform')
                ->pluck('source_platform'),
            'categoryNames' => EbayListing::query()
                ->whereNotNull('category_name')
                ->distinct()
                ->orderBy('category_name')
                ->pluck('category_name'),
        ]);
    }
}
