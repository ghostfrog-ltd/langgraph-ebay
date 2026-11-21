<?php

namespace App\Livewire\Links;

use App\Models\ThreeDPrintSeller;
use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Component;

#[Title('3D print leaderboard')]
class ThreeDPrintLeaderboard extends Component
{
    public function render(): View
    {
        $sellers = ThreeDPrintSeller::query()
            ->orderBy('rank')
            ->limit(50)
            ->get();

        return view('livewire.links.three-d-print-leaderboard', [
            'sellers' => $sellers,
            'sellerCount' => ThreeDPrintSeller::query()->count(),
            'itemCount' => ThreeDPrintSeller::query()->sum('item_count'),
            'activeValue' => ThreeDPrintSeller::query()->sum('active_value'),
            'latestSnapshot' => ThreeDPrintSeller::query()->max('snapshot_at'),
        ]);
    }
}
