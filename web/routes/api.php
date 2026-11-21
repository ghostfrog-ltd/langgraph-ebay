<?php

use App\Http\Controllers\Api\ListingSyncController;
use App\Http\Controllers\Api\ModelKeyCompSyncController;
use App\Http\Controllers\Api\SoldEvidenceSyncController;
use App\Http\Controllers\Api\ThreeDPrintSellerSyncController;
use Illuminate\Support\Facades\Route;

Route::post('v1/sync/listings', ListingSyncController::class)
    ->name('api.sync.listings');

Route::post('v1/sync/model-key-comps', ModelKeyCompSyncController::class)
    ->name('api.sync.model-key-comps');

Route::post('v1/sync/sold-evidence', SoldEvidenceSyncController::class)
    ->name('api.sync.sold-evidence');

Route::post('v1/sync/3d-print-sellers', ThreeDPrintSellerSyncController::class)
    ->name('api.sync.3d-print-sellers');
