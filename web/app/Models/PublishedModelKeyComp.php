<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Attributes\Fillable;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

#[Fillable([
    'source',
    'model_key',
    'condition_grade',
    'samples',
    'median_final_price',
    'computed_at',
    'synced_at',
])]
class PublishedModelKeyComp extends Model
{
    protected function casts(): array
    {
        return [
            'samples' => 'integer',
            'median_final_price' => 'decimal:2',
            'computed_at' => 'datetime',
            'synced_at' => 'datetime',
        ];
    }

    /** @return HasMany<PublishedModelKeyCompRow, $this> */
    public function rows(): HasMany
    {
        return $this->hasMany(PublishedModelKeyCompRow::class);
    }
}
