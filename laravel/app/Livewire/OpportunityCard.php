<?php

namespace App\Livewire;

use App\Models\EbayListing;
use Illuminate\Contracts\View\View;
use Livewire\Component;

class OpportunityCard extends Component
{
    public EbayListing $listing;

    public function dismiss(): void
    {
        $this->listing->update([
            'pipeline_state' => 'dismissed',
        ]);

        $this->dispatch('opportunity-dismissed');
        $this->dispatch('pipeline-status-changed');
    }

    public function render(): View
    {
        return view('livewire.opportunity-card');
    }
}
