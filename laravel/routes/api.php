<?php

use App\Http\Controllers\Api\ListingIngestionController;
use App\Http\Middleware\EnsureValidPipelineToken;
use Illuminate\Support\Facades\Route;

Route::middleware(EnsureValidPipelineToken::class)->group(function () {
    Route::post('pipeline/listings', [ListingIngestionController::class, 'store'])
        ->name('api.pipeline.listings.store');
});
