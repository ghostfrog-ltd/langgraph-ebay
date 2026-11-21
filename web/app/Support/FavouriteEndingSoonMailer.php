<?php

namespace App\Support;

use App\Models\ListingFavourite;
use App\Models\PublishedListing;
use App\Models\User;
use App\Notifications\FavouriteEndingSoonNotification;
use Carbon\CarbonInterface;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Throwable;

class FavouriteEndingSoonMailer
{
    /**
     * @return array{eligible:int, sent:int, skipped:int, failed:int, dry_run:bool}
     */
    public function send(int $hours = 24, bool $dryRun = false): array
    {
        $hours = max(1, $hours);
        $now = now();
        $until = $now->copy()->addHours($hours);
        $windowStart = $now->copy()->startOfHour();

        $stats = [
            'eligible' => 0,
            'sent' => 0,
            'skipped' => 0,
            'failed' => 0,
            'dry_run' => $dryRun,
        ];

        ListingFavourite::query()
            ->with(['user', 'currentListing'])
            ->whereHas('currentListing', function (Builder $query) use ($now, $until): void {
                $query->whereBetween('end_time', [$now, $until]);
            })
            ->orderBy(
                PublishedListing::query()
                    ->select('end_time')
                    ->whereColumn('published_listings.external_id', 'listing_favourites.external_id')
                    ->limit(1)
            )
            ->chunkById(100, function ($favourites) use (&$stats, $now, $until, $windowStart, $dryRun): void {
                foreach ($favourites as $favourite) {
                    /** @var ListingFavourite $favourite */
                    $user = $favourite->user;
                    $listing = $favourite->currentListing;
                    $endTime = $this->effectiveEndTime($favourite, $listing);

                    if (
                        ! $user instanceof User
                        || ! $listing instanceof PublishedListing
                        || trim((string) $user->email) === ''
                        || ! $this->isEndingWithinWindow($endTime, $now, $until)
                    ) {
                        $stats['skipped']++;

                        continue;
                    }

                    $stats['eligible']++;

                    if ($dryRun) {
                        $stats['sent']++;

                        continue;
                    }

                    $inserted = DB::table('favourite_ending_soon_notifications')->insertOrIgnore([
                        'user_id' => $user->id,
                        'listing_favourite_id' => $favourite->id,
                        'external_id' => $favourite->external_id,
                        'listing_end_time' => $endTime,
                        'notification_window_start' => $windowStart,
                        'sent_at' => now(),
                        'created_at' => now(),
                        'updated_at' => now(),
                    ]);

                    if ($inserted === 0) {
                        $stats['skipped']++;

                        continue;
                    }

                    try {
                        $user->notify(new FavouriteEndingSoonNotification($favourite, $listing));
                        $stats['sent']++;
                    } catch (Throwable $exception) {
                        $stats['failed']++;

                        DB::table('favourite_ending_soon_notifications')
                            ->where('listing_favourite_id', $favourite->id)
                            ->where('notification_window_start', $windowStart)
                            ->delete();

                        report($exception);
                    }
                }
            });

        return $stats;
    }

    private function effectiveEndTime(ListingFavourite $favourite, ?PublishedListing $listing): ?CarbonInterface
    {
        $endTime = $listing?->end_time ?? $favourite->end_time;

        if ($endTime instanceof CarbonInterface) {
            return $endTime;
        }

        return $endTime ? Carbon::parse($endTime) : null;
    }

    private function isEndingWithinWindow(?CarbonInterface $endTime, CarbonInterface $now, CarbonInterface $until): bool
    {
        return $endTime instanceof CarbonInterface
            && $endTime->greaterThan($now)
            && $endTime->lessThanOrEqualTo($until);
    }
}
