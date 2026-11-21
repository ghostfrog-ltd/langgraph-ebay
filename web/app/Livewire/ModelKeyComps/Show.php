<?php

namespace App\Livewire\ModelKeyComps;

use App\Models\PublishedModelKeyComp;
use Illuminate\Support\Collection;
use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Component;

#[Title('Model-key evidence')]
class Show extends Component
{
    public PublishedModelKeyComp $modelKeyComp;

    /** @var Collection<int, PublishedModelKeyComp> */
    public Collection $gradeSummaries;

    public function mount(PublishedModelKeyComp $modelKeyComp): void
    {
        $this->modelKeyComp = $modelKeyComp;
        $this->gradeSummaries = PublishedModelKeyComp::query()
            ->where('model_key', $modelKeyComp->model_key)
            ->orderBy('condition_grade')
            ->get();
    }

    public function render(): View
    {
        $median = (float) $this->modelKeyComp->median_final_price;
        $rows = $this->modelKeyComp->rows()
            ->get()
            ->sortBy(fn ($row): array => [
                $row->visual_condition_checked ? 0 : 1,
                $row->visual_review_required ? 1 : 0,
                abs((float) $row->final_price - $median),
                (float) $row->final_price,
            ])
            ->values();

        return view('livewire.model-key-comps.show', [
            'rows' => $rows,
            'math' => $this->math($median),
        ]);
    }

    /** @return array<string, float> */
    private function math(float $median): array
    {
        $fees = round($median * 0.13, 2);
        $outboundPostage = 7.0;
        $inboundPostage = 5.0;
        $packaging = 2.0;
        $sellingReserve = 3.0;
        $netOwned = round($median - $fees - $outboundPostage - $packaging - $sellingReserve, 2);
        $maxSafeBuy = round($median - $fees - $outboundPostage - $inboundPostage - $packaging - $sellingReserve - 50.0, 2);

        return [
            'fees' => $fees,
            'outboundPostage' => $outboundPostage,
            'inboundPostage' => $inboundPostage,
            'packaging' => $packaging,
            'sellingReserve' => $sellingReserve,
            'netOwned' => $netOwned,
            'maxSafeBuy' => $maxSafeBuy,
            'suggestedBin' => round($median * 0.95),
            'offerFloor' => round($median * 0.95) - 25,
        ];
    }
}
