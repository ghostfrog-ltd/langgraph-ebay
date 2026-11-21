<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Attributes\Fillable;
use Illuminate\Database\Eloquent\Model;

#[Fillable([
    'rank',
    'marketplace',
    'seller_username',
    'item_count',
    'active_value',
    'max_price',
    'feedback_score',
    'feedback_percentage',
    'matched_queries',
    'example_titles',
    'example_url',
    'sync_batch_id',
    'snapshot_at',
])]
class ThreeDPrintSeller extends Model
{
    protected function casts(): array
    {
        return [
            'active_value' => 'decimal:2',
            'max_price' => 'decimal:2',
            'feedback_percentage' => 'decimal:2',
            'matched_queries' => 'array',
            'example_titles' => 'array',
            'snapshot_at' => 'datetime',
        ];
    }

    public function sellerUrl(): string
    {
        return 'https://www.ebay.co.uk/usr/'.rawurlencode($this->seller_username);
    }
}
