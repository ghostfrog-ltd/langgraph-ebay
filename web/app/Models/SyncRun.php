<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Attributes\Fillable;
use Illuminate\Database\Eloquent\Model;

#[Fillable([
    'batch_id', 'generated_at', 'received_at', 'listing_count', 'comp_count',
    'payload_sha256', 'summary',
])]
class SyncRun extends Model
{
    protected function casts(): array
    {
        return [
            'generated_at' => 'datetime',
            'received_at' => 'datetime',
            'summary' => 'array',
        ];
    }
}
