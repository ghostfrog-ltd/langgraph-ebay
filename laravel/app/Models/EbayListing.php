<?php

namespace App\Models;

use Database\Factories\EbayListingFactory;
use Illuminate\Database\Eloquent\Attributes\UseFactory;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

#[UseFactory(EbayListingFactory::class)]
class EbayListing extends Model
{
    use HasFactory;

    protected $fillable = [
        'ebay_id',
        'source_platform',
        'category_name',
        'title',
        'image_url',
        'current_price',
        'estimated_market_value',
        'item_url',
        'llm_analysis',
        'attributes_json',
        'pipeline_state',
        'last_synced_at',
    ];

    protected $appends = [
        'roi_percentage',
    ];

    protected function casts(): array
    {
        return [
            'current_price' => 'decimal:2',
            'estimated_market_value' => 'decimal:2',
            'attributes_json' => 'array',
            'last_synced_at' => 'datetime',
        ];
    }

    public function getRoiPercentageAttribute(): float
    {
        $currentPrice = (float) $this->current_price;
        $estimatedMarketValue = (float) $this->estimated_market_value;

        if ($currentPrice <= 0.0) {
            return 0.0;
        }

        return round((($estimatedMarketValue - $currentPrice) / $currentPrice) * 100, 2);
    }

    public function scopeHotOpportunities(Builder $query): Builder
    {
        return $query->where('pipeline_state', 'opportunity')
            ->where('current_price', '>', 0)
            ->whereRaw('((estimated_market_value - current_price) * 100.0) / current_price > ?', [15]);
    }
}
