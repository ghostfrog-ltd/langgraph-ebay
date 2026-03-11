<?php

namespace App\Livewire;

use App\Models\EbayListing;
use Illuminate\Contracts\View\View;
use Livewire\Attributes\On;
use Livewire\Component;

class PipelineStatus extends Component
{
    #[On('pipeline-status-changed')]
    public function refreshStatus(): void
    {
        // Re-render when pipeline counts change.
    }

    public function purgeDismissed(): void
    {
        EbayListing::query()
            ->where('pipeline_state', 'dismissed')
            ->delete();

        $this->dispatch('pipeline-status-changed');
        $this->dispatch('opportunity-dismissed');
    }

    public function render(): View
    {
        $counts = EbayListing::query()
            ->selectRaw('pipeline_state, count(*) as aggregate')
            ->groupBy('pipeline_state')
            ->pluck('aggregate', 'pipeline_state');

        $states = [
            'ingested' => 'Ingested',
            'analyzing' => 'Analyzing',
            'opportunity' => 'Opportunities',
            'dismissed' => 'Dismissed',
        ];

        return view('livewire.pipeline-status', [
            'states' => $states,
            'counts' => $counts,
            'lastSyncedAt' => EbayListing::query()->max('last_synced_at'),
        ]);
    }
}
