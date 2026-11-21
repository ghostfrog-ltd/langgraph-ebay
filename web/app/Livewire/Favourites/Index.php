<?php

namespace App\Livewire\Favourites;

use App\Models\ListingFavourite;
use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Component;
use Livewire\WithPagination;

#[Title('Favourites')]
class Index extends Component
{
    use WithPagination;

    public function render(): View
    {
        /** @var LengthAwarePaginator<int, ListingFavourite> $favourites */
        $favourites = ListingFavourite::query()
            ->with('currentListing')
            ->where('user_id', auth()->id())
            ->orderByRaw(
                'CASE WHEN end_time IS NOT NULL AND end_time >= ? THEN 0 WHEN end_time IS NULL THEN 1 ELSE 2 END',
                [now()],
            )
            ->orderByRaw('end_time IS NULL')
            ->orderBy('end_time')
            ->orderByDesc('favourited_at')
            ->paginate(24);

        return view('livewire.favourites.index', [
            'favourites' => $favourites,
            'favouriteCount' => ListingFavourite::query()
                ->where('user_id', auth()->id())
                ->count(),
        ]);
    }
}
