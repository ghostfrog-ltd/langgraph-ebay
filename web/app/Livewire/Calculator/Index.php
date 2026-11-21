<?php

namespace App\Livewire\Calculator;

use Illuminate\View\View;
use Livewire\Attributes\Title;
use Livewire\Component;

#[Title('Sale calculator')]
class Index extends Component
{
    public float|int|string $sellPrice = 640;

    public float|int|string $feeRate = 13;

    public float|int|string $outboundPostage = 7;

    public float|int|string $inboundPostage = 5;

    public float|int|string $packaging = 2;

    public float|int|string $sellingReserve = 3;

    public float|int|string $buyPrice = 0;

    public bool $ownedItem = true;

    public function resetDefaults(): void
    {
        $this->sellPrice = 640;
        $this->feeRate = 13;
        $this->outboundPostage = 7;
        $this->inboundPostage = 5;
        $this->packaging = 2;
        $this->sellingReserve = 3;
        $this->buyPrice = 0;
        $this->ownedItem = true;
    }

    public function render(): View
    {
        return view('livewire.calculator.index', [
            'math' => $this->math(),
        ]);
    }

    /** @return array<string, float|bool> */
    private function math(): array
    {
        $sellPrice = $this->money($this->sellPrice);
        $feeRate = max($this->number($this->feeRate), 0);
        $fees = $this->money($sellPrice * ($feeRate / 100));
        $outboundPostage = $this->money($this->outboundPostage);
        $inboundPostage = $this->ownedItem ? 0.0 : $this->money($this->inboundPostage);
        $packaging = $this->money($this->packaging);
        $sellingReserve = $this->money($this->sellingReserve);
        $buyPrice = $this->ownedItem ? 0.0 : $this->money($this->buyPrice);
        $totalCosts = $this->money($fees + $outboundPostage + $inboundPostage + $packaging + $sellingReserve + $buyPrice);
        $net = $this->money($sellPrice - $totalCosts);
        $roi = $buyPrice > 0 ? round(($net / $buyPrice) * 100, 1) : 0.0;

        return [
            'sellPrice' => $sellPrice,
            'feeRate' => $feeRate,
            'fees' => $fees,
            'outboundPostage' => $outboundPostage,
            'inboundPostage' => $inboundPostage,
            'packaging' => $packaging,
            'sellingReserve' => $sellingReserve,
            'buyPrice' => $buyPrice,
            'totalCosts' => $totalCosts,
            'net' => $net,
            'roi' => $roi,
            'ownedItem' => $this->ownedItem,
        ];
    }

    private function number(mixed $value): float
    {
        if (is_numeric($value)) {
            return (float) $value;
        }

        return 0.0;
    }

    private function money(mixed $value): float
    {
        return round(max($this->number($value), 0), 2);
    }
}
