<?php

namespace App\Livewire\Listings;

use App\Models\ListingFavourite;
use App\Models\PublishedComp;
use App\Models\PublishedListing;
use Illuminate\Support\Collection;
use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Component;

#[Title('Listing evidence')]
class Show extends Component
{
    public PublishedListing $listing;

    /** @var Collection<int, PublishedComp> */
    public Collection $comps;

    public ?ListingFavourite $favourite = null;

    public function mount(PublishedListing $listing): void
    {
        $this->listing = $listing;
        $median = (float) $listing->sold_median;
        $this->comps = $listing->comps()
            ->get()
            ->sortBy(fn ($comp): array => [
                $comp->visual_condition_checked ? 0 : 1,
                $comp->visual_review_required ? 1 : 0,
                abs((float) $comp->final_price - $median),
                (float) $comp->final_price,
            ])
            ->values();
        $this->favourite = ListingFavourite::query()
            ->where('user_id', auth()->id())
            ->where('external_id', $listing->external_id)
            ->first();
    }

    public function render(): View
    {
        return view('livewire.listings.show');
    }
}
