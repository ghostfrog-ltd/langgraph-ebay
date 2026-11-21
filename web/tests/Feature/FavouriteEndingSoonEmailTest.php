<?php

namespace Tests\Feature;

use App\Models\ListingFavourite;
use App\Models\PublishedListing;
use App\Models\User;
use App\Notifications\FavouriteEndingSoonNotification;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Notification;
use Tests\TestCase;

class FavouriteEndingSoonEmailTest extends TestCase
{
    use RefreshDatabase;

    public function test_hourly_command_emails_users_about_favourites_ending_soon_once_per_hour(): void
    {
        $this->travelTo(now()->startOfHour()->addMinutes(10));

        Notification::fake();

        $user = User::factory()->create(['email' => 'buyer@example.test']);
        $listing = $this->publishListing([
            'external_id' => 'ending-soon-123',
            'title' => 'Apple iPhone 16 Pro ending soon',
            'end_time' => now()->addHours(2),
            'current_price' => 300,
            'expected_profit' => 75,
        ]);
        $favourite = $this->favouriteListing($user, $listing);

        $this->artisan('ghostfrog:favourites-ending-soon')
            ->assertExitCode(0);

        Notification::assertSentTo(
            $user,
            FavouriteEndingSoonNotification::class,
            fn (FavouriteEndingSoonNotification $notification): bool => $notification->favourite->is($favourite)
                && $notification->currentListing?->is($listing),
        );

        $this->assertDatabaseHas('favourite_ending_soon_notifications', [
            'user_id' => $user->id,
            'listing_favourite_id' => $favourite->id,
            'external_id' => 'ending-soon-123',
        ]);

        Notification::fake();

        $this->artisan('ghostfrog:favourites-ending-soon')
            ->assertExitCode(0);

        Notification::assertNothingSent();
    }

    public function test_hourly_command_ignores_ended_and_far_future_favourites(): void
    {
        Notification::fake();

        $user = User::factory()->create();

        $ended = $this->publishListing([
            'external_id' => 'already-ended-123',
            'title' => 'Already ended favourite',
            'end_time' => now()->subMinute(),
        ]);
        $future = $this->publishListing([
            'external_id' => 'far-future-123',
            'title' => 'Far future favourite',
            'end_time' => now()->addHours(25),
        ]);

        $this->favouriteListing($user, $ended);
        $this->favouriteListing($user, $future);

        $this->artisan('ghostfrog:favourites-ending-soon')
            ->assertExitCode(0);

        Notification::assertNothingSent();
        $this->assertDatabaseCount('favourite_ending_soon_notifications', 0);
    }

    public function test_hourly_command_ignores_favourites_that_are_no_longer_live(): void
    {
        Notification::fake();

        $user = User::factory()->create();

        ListingFavourite::query()->create([
            'user_id' => $user->id,
            'external_id' => 'no-longer-live-123',
            'source' => 'ebay-apple',
            'title' => 'Favourite with stale future snapshot',
            'url' => 'https://www.ebay.co.uk/itm/no-longer-live-123',
            'model_key' => 'apple-iphone-stale',
            'condition_grade' => 'B',
            'sale_type' => 'auction',
            'end_time' => now()->addHours(2),
            'current_price' => 100,
            'sold_median' => 200,
            'comp_samples' => 10,
            'max_safe_buy' => 120,
            'expected_profit' => 20,
            'expected_roi' => 0.2,
            'price_gap_to_max_buy' => 20,
            'effective_min_comps' => 10,
            'signal' => 'saved snapshot only',
            'comps_snapshot' => [],
            'favourited_at' => now(),
        ]);

        $this->artisan('ghostfrog:favourites-ending-soon')
            ->assertExitCode(0);

        Notification::assertNothingSent();
        $this->assertDatabaseCount('favourite_ending_soon_notifications', 0);
    }

    /**
     * @param  array<string, mixed>  $overrides
     */
    private function publishListing(array $overrides = []): PublishedListing
    {
        return PublishedListing::query()->create(array_merge([
            'external_id' => 'listing-123',
            'source' => 'ebay-apple',
            'title' => 'Apple iPhone 16 Pro',
            'url' => 'https://www.ebay.co.uk/itm/listing-123',
            'model_key' => 'apple-iphone-16-pro-256gb',
            'condition_grade' => 'B',
            'sale_type' => 'auction',
            'end_time' => now()->addHours(2),
            'current_price' => 350,
            'sold_median' => 550,
            'comp_samples' => 42,
            'max_safe_buy' => 420,
            'expected_profit' => 70,
            'expected_roi' => 0.20,
            'price_gap_to_max_buy' => 70,
            'effective_min_comps' => 10,
            'signal' => 'confirmed same-grade sold evidence',
            'sync_batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'published_at' => now(),
        ], $overrides));
    }

    private function favouriteListing(User $user, PublishedListing $listing): ListingFavourite
    {
        return ListingFavourite::query()->create([
            'user_id' => $user->id,
            'external_id' => $listing->external_id,
            'source' => $listing->source,
            'title' => $listing->title,
            'url' => $listing->url,
            'model_key' => $listing->model_key,
            'condition_grade' => $listing->condition_grade,
            'sale_type' => $listing->sale_type,
            'end_time' => $listing->end_time,
            'current_price' => $listing->current_price,
            'sold_median' => $listing->sold_median,
            'comp_samples' => $listing->comp_samples,
            'max_safe_buy' => $listing->max_safe_buy,
            'expected_profit' => $listing->expected_profit,
            'expected_roi' => $listing->expected_roi,
            'price_gap_to_max_buy' => $listing->price_gap_to_max_buy,
            'effective_min_comps' => $listing->effective_min_comps,
            'signal' => $listing->signal,
            'comps_snapshot' => [],
            'favourited_at' => now(),
        ]);
    }
}
