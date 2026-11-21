<?php

namespace App\Livewire\Favourites;

use App\Models\ListingFavourite;
use Illuminate\Support\Collection;
use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Component;

#[Title('Favourite evidence')]
class Show extends Component
{
    public ListingFavourite $favourite;

    /** @var Collection<int, array<string, mixed>> */
    public Collection $comps;

    public function mount(ListingFavourite $favourite): void
    {
        abort_if($favourite->user_id !== auth()->id(), 404);

        $this->favourite = $favourite;
        $median = (float) $favourite->sold_median;
        $this->comps = collect($favourite->comps_snapshot ?? [])
            ->sortBy(fn (array $comp): array => [
                (bool) ($comp['visual_condition_checked'] ?? false) ? 0 : 1,
                (bool) ($comp['visual_review_required'] ?? false) ? 1 : 0,
                abs((float) ($comp['final_price'] ?? 0) - $median),
                (float) ($comp['final_price'] ?? 0),
            ])
            ->values();
    }

    public function render(): View
    {
        return view('livewire.favourites.show');
    }
}
