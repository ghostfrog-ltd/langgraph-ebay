<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Attributes\Fillable;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

#[Fillable([
    'published_model_key_comp_id',
    'comp_external_id',
    'source',
    'title',
    'url',
    'sale_type',
    'end_time',
    'final_price',
    'bids_count',
    'text_scan_grade',
    'text_scan_checked',
    'text_scan_review_required',
    'text_scan_risk_flags',
    'text_scan_model_name',
    'text_scan_checked_at',
    'visual_condition_grade',
    'visual_condition_checked',
    'visual_review_required',
    'visual_damage_flags',
    'visual_model_name',
    'visual_checked_at',
])]
class PublishedModelKeyCompRow extends Model
{
    protected function casts(): array
    {
        return [
            'end_time' => 'datetime',
            'final_price' => 'decimal:2',
            'text_scan_checked' => 'boolean',
            'text_scan_review_required' => 'boolean',
            'text_scan_risk_flags' => 'array',
            'text_scan_checked_at' => 'datetime',
            'visual_condition_checked' => 'boolean',
            'visual_review_required' => 'boolean',
            'visual_damage_flags' => 'array',
            'visual_checked_at' => 'datetime',
        ];
    }

    /** @return BelongsTo<PublishedModelKeyComp, $this> */
    public function summary(): BelongsTo
    {
        return $this->belongsTo(PublishedModelKeyComp::class, 'published_model_key_comp_id');
    }
}
