<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Attributes\Fillable;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

#[Fillable([
    'external_id', 'source', 'title', 'url', 'model_key', 'condition_grade',
    'text_condition_grade', 'text_scan_grade', 'text_scan_checked',
    'text_scan_review_required', 'text_scan_risk_flags',
    'text_scan_model_name', 'text_scan_checked_at', 'visual_condition_grade',
    'visual_condition_checked', 'visual_review_required', 'visual_damage_flags',
    'visual_model_name', 'visual_checked_at', 'sale_type', 'end_time',
    'current_price', 'sold_median', 'comp_samples', 'max_safe_buy',
    'expected_profit', 'expected_roi', 'price_gap_to_max_buy',
    'effective_min_comps', 'signal', 'sync_batch_id', 'published_at',
])]
class PublishedListing extends Model
{
    protected $primaryKey = 'external_id';

    public $incrementing = false;

    protected $keyType = 'string';

    protected function casts(): array
    {
        return [
            'end_time' => 'datetime',
            'published_at' => 'datetime',
            'text_scan_checked' => 'boolean',
            'text_scan_review_required' => 'boolean',
            'text_scan_risk_flags' => 'array',
            'text_scan_checked_at' => 'datetime',
            'visual_condition_checked' => 'boolean',
            'visual_review_required' => 'boolean',
            'visual_damage_flags' => 'array',
            'visual_checked_at' => 'datetime',
            'current_price' => 'decimal:2',
            'sold_median' => 'decimal:2',
            'max_safe_buy' => 'decimal:2',
            'expected_profit' => 'decimal:2',
            'expected_roi' => 'decimal:4',
            'price_gap_to_max_buy' => 'decimal:2',
        ];
    }

    /** @return HasMany<PublishedComp, $this> */
    public function comps(): HasMany
    {
        return $this->hasMany(PublishedComp::class, 'listing_external_id');
    }
}
