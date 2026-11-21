<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Attributes\Fillable;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;

/**
 * @property string $external_id
 * @property string $source
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
 * @property Carbon $synced_at
 * @property string|null $sync_batch_id
 * @property Carbon|null $created_at
 * @property Carbon|null $updated_at
 */
#[Fillable([
    'external_id',
    'source',
    'title',
    'url',
    'sale_type',
    'end_time',
    'final_price',
    'bids_count',
    'condition_grade',
    'model_key',
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
    'synced_at',
    'sync_batch_id',
])]
class PublishedSoldEvidence extends Model
{
    protected $primaryKey = 'external_id';

    public $incrementing = false;

    protected $keyType = 'string';

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
            'synced_at' => 'datetime',
        ];
    }
}
