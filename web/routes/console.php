<?php

use App\Support\FavouriteEndingSoonMailer;
use Illuminate\Foundation\Inspiring;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Schedule;

Artisan::command('inspire', function () {
    $this->comment(Inspiring::quote());
})->purpose('Display an inspiring quote');

Artisan::command('ghostfrog:favourites-ending-soon {--hours=24 : Favourite listings ending within this many hours} {--dry-run : Count eligible emails without sending}', function () {
    $stats = app(FavouriteEndingSoonMailer::class)->send(
        hours: (int) $this->option('hours'),
        dryRun: (bool) $this->option('dry-run'),
    );

    $this->components->info(sprintf(
        'Favourite ending-soon emails: %d eligible, %d %s, %d skipped, %d failed.',
        $stats['eligible'],
        $stats['sent'],
        $stats['dry_run'] ? 'would send' : 'sent',
        $stats['skipped'],
        $stats['failed'],
    ));

    return $stats['failed'] > 0 ? 1 : 0;
})->purpose('Email users about favourited listings ending soon');

Schedule::command('ghostfrog:favourites-ending-soon')
    ->hourly()
    ->withoutOverlapping();
