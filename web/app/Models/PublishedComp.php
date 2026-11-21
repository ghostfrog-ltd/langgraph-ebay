<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Attributes\Fillable;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property int $id
 * @property string $listing_external_id
 * @property string $comp_external_id
 * @property string|null $source
 * @property string $title
 * @property string|null $url
 * @property string|null $sale_type
 * @property Carbon|null $end_time
 * @property string $final_price
 * @property int|null $bids_count
 * @property string|null $condition_grade
 * @property string|null $model_key
 * @property string|null $text_scan_grade
 * @property bool $text_scan_checked
 * @property bool $text_scan_review_required
 * @property array<int, string>|null $text_scan_risk_flags
 * @property string|null $text_scan_model_name
 * @property Carbon|null $text_scan_checked_at
 * @property string|null $visual_condition_grade
 * @property bool $visual_condition_checked
 * @property bool $visual_review_required
 * @property array<int, string>|null $visual_damage_flags
 * @property string|null $visual_model_name
 * @property Carbon|null $visual_checked_at
 * @property Carbon|null $created_at
 * @property Carbon|null $updated_at
 * @property-read PublishedListing $listing
 */
#[Fillable([
    'listing_external_id', 'comp_external_id', 'source', 'title', 'url',
    'sale_type', 'end_time', 'final_price', 'bids_count', 'condition_grade',
    'model_key', 'text_scan_grade', 'text_scan_checked',
    'text_scan_review_required', 'text_scan_risk_flags',
    'text_scan_model_name', 'text_scan_checked_at',
    'visual_condition_grade', 'visual_condition_checked',
    'visual_review_required', 'visual_damage_flags', 'visual_model_name',
    'visual_checked_at',
])]
class PublishedComp extends Model
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

    /** @return BelongsTo<PublishedListing, $this> */
    public function listing(): BelongsTo
    {
        return $this->belongsTo(PublishedListing::class, 'listing_external_id');
    }
}
