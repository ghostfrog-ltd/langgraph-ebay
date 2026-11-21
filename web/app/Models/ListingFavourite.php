<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Attributes\Fillable;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property int $id
 * @property int $user_id
 * @property string $external_id
 * @property string $source
 * @property string $title
 * @property string $url
 * @property string $model_key
 * @property string $condition_grade
 * @property string|null $text_condition_grade
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
 * @property string|null $sale_type
 * @property Carbon|null $end_time
 * @property string $current_price
 * @property string $sold_median
 * @property int $comp_samples
 * @property string $max_safe_buy
 * @property string $expected_profit
 * @property string $expected_roi
 * @property string $price_gap_to_max_buy
 * @property int $effective_min_comps
 * @property string $signal
 * @property array<int, array<string, mixed>>|null $comps_snapshot
 * @property Carbon $favourited_at
 * @property Carbon|null $created_at
 * @property Carbon|null $updated_at
 * @property-read PublishedListing|null $currentListing
 * @property-read User $user
 */
#[Fillable([
    'user_id', 'external_id', 'source', 'title', 'url', 'model_key',
    'condition_grade', 'text_condition_grade', 'text_scan_grade',
    'text_scan_checked', 'text_scan_review_required', 'text_scan_risk_flags',
    'text_scan_model_name', 'text_scan_checked_at', 'visual_condition_grade',
    'visual_condition_checked', 'visual_review_required', 'visual_damage_flags',
    'visual_model_name', 'visual_checked_at', 'sale_type', 'end_time',
    'current_price', 'sold_median', 'comp_samples', 'max_safe_buy',
    'expected_profit', 'expected_roi', 'price_gap_to_max_buy',
    'effective_min_comps', 'signal', 'comps_snapshot', 'favourited_at',
])]
class ListingFavourite extends Model
{
    protected function casts(): array
    {
        return [
            'end_time' => 'datetime',
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
            'comps_snapshot' => 'array',
            'favourited_at' => 'datetime',
        ];
    }

    /** @return BelongsTo<User, $this> */
    public function user(): BelongsTo
    {
        return $this->belongsTo(User::class);
    }

    /** @return BelongsTo<PublishedListing, $this> */
    public function currentListing(): BelongsTo
    {
        return $this->belongsTo(PublishedListing::class, 'external_id', 'external_id');
    }
}
