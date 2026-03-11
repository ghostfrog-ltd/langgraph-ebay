<?php

use App\Actions\SyncPythonAuctionListings;
use App\Actions\SyncPythonOpportunityEnrichment;
use Illuminate\Foundation\Inspiring;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Schedule;

Artisan::command('inspire', function () {
    $this->comment(Inspiring::quote());
})->purpose('Display an inspiring quote');

Artisan::command('listings:sync-python', function () {
    try {
        $count = app(SyncPythonAuctionListings::class)->run();
    } catch (\Throwable $e) {
        $this->error('Python listing sync failed: '.$e->getMessage());

        return self::FAILURE;
    }

    $this->info("Synced {$count} listing(s) from the Python database.");
})->purpose('Sync raw auction listings from the Python Postgres database into Laravel.');

Artisan::command('listings:sync-opportunities', function () {
    try {
        $count = app(SyncPythonOpportunityEnrichment::class)->run();
    } catch (\Throwable $e) {
        $this->error('Python opportunity sync failed: '.$e->getMessage());

        return self::FAILURE;
    }

    $this->info("Synced {$count} opportunity enrichment row(s) from the Python database.");
})->purpose('Sync ROI and assessment enrichment from the Python Postgres database into Laravel.');

Schedule::command('listings:sync-python')
    ->everyFiveMinutes()
    ->withoutOverlapping();

Schedule::command('listings:sync-opportunities')
    ->everyFiveMinutes()
    ->withoutOverlapping();
