<?php

namespace Tests\Feature;

use App\Livewire\Calculator\Index;
use App\Models\ListingFavourite;
use App\Models\PublishedComp;
use App\Models\PublishedListing;
use App\Models\PublishedModelKeyComp;
use App\Models\PublishedModelKeyCompRow;
use App\Models\PublishedSoldEvidence;
use App\Models\ThreeDPrintSeller;
use App\Models\User;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Livewire\Livewire;
use Tests\TestCase;

class DashboardTest extends TestCase
{
    use RefreshDatabase;

    public function test_guests_are_redirected_to_the_login_page(): void
    {
        $response = $this->get(route('dashboard'));
        $response->assertRedirect(route('login'));
    }

    public function test_authenticated_users_can_visit_the_dashboard(): void
    {
        $user = User::factory()->create();
        $this->actingAs($user);

        $response = $this->get(route('dashboard'));
        $response->assertOk();
    }

    public function test_sidebar_hides_the_3d_print_leaderboard(): void
    {
        $user = User::factory()->create();

        $this->actingAs($user)
            ->get(route('dashboard'))
            ->assertOk()
            ->assertDontSee('Links')
            ->assertDontSee('3D print leaderboard')
            ->assertDontSee(route('links.3d-print-leaderboard'), false);
    }

    public function test_authenticated_users_can_view_the_3d_print_leaderboard(): void
    {
        $user = User::factory()->create();

        ThreeDPrintSeller::query()->create([
            'rank' => 1,
            'marketplace' => 'EBAY_GB',
            'seller_username' => 'print_pioneers',
            'item_count' => 145,
            'active_value' => 7385.75,
            'max_price' => 139.95,
            'feedback_score' => 2256,
            'feedback_percentage' => 99.4,
            'matched_queries' => ['3d printed car', '3d printed rc'],
            'example_titles' => [
                'Plymouth Fury 1958 Model Car Kit 3D Printed',
                'Porsche 911 GT3 Touring Model Car Kit 3D Printed',
            ],
            'example_url' => 'https://www.ebay.co.uk/itm/127894837271',
            'sync_batch_id' => 'bfc8cb0c-b357-4137-98b6-88eb08abe155',
            'snapshot_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('links.3d-print-leaderboard'))
            ->assertOk()
            ->assertSee('3D print leaderboard')
            ->assertSee('print_pioneers')
            ->assertSee('Plymouth Fury 1958 Model Car Kit')
            ->assertSee('£7,385.75')
            ->assertSee('visible inventory depth');
    }

    public function test_authenticated_users_can_use_the_sale_calculator(): void
    {
        $user = User::factory()->create();

        $this->actingAs($user)
            ->get(route('calculator.index'))
            ->assertOk()
            ->assertSee('Sale calculator')
            ->assertSee('Net after fees')
            ->assertSee('£544.80');

        Livewire::actingAs($user)
            ->test(Index::class)
            ->set('sellPrice', 625)
            ->assertSee('£531.75')
            ->set('ownedItem', false)
            ->set('buyPrice', 490)
            ->assertSee('£36.75');
    }

    public function test_sidebar_shows_counts_and_hides_sources_without_visible_rows(): void
    {
        config([
            'services.ghostfrog.sources' => [
                'ebay-apple',
                'ebay-lego',
                'ebay-tools',
                'motomine',
                'ebay-motors',
            ],
            'services.ghostfrog.hidden_sources' => [
                'ebay-motors',
            ],
        ]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'apple-visible-one',
            'source' => 'ebay-apple',
            'title' => 'Visible Apple one',
            'comp_samples' => 3,
            'max_safe_buy' => 100,
        ]);

        $this->publishListing([
            'external_id' => 'apple-visible-two',
            'source' => 'ebay-apple',
            'title' => 'Visible Apple two',
            'comp_samples' => 1,
            'max_safe_buy' => 50,
        ]);

        $this->publishListing([
            'external_id' => 'lego-without-comps',
            'source' => 'ebay-lego',
            'title' => 'Lego row without comps',
            'comp_samples' => 0,
            'max_safe_buy' => 50,
        ]);

        $this->publishListing([
            'external_id' => 'tools-negative-safe-buy',
            'source' => 'ebay-tools',
            'title' => 'Tools row with negative safe buy',
            'comp_samples' => 2,
            'max_safe_buy' => -5,
        ]);

        $this->publishListing([
            'external_id' => 'motomine-without-comps',
            'source' => 'motomine',
            'title' => 'Motomine row without comps',
            'comp_samples' => 0,
            'max_safe_buy' => 0,
        ]);

        $this->publishListing([
            'external_id' => 'hidden-motor-sidebar',
            'source' => 'ebay-motors',
            'title' => 'Hidden motor sidebar row',
            'comp_samples' => 10,
            'max_safe_buy' => 200,
        ]);

        $this->actingAs($user);

        $this->get(route('dashboard'))
            ->assertOk()
            ->assertSee('Apple')
            ->assertSee('(2)', false)
            ->assertSee('Motomine')
            ->assertSee('(1)', false)
            ->assertDontSee('Lego')
            ->assertDontSee('Tools')
            ->assertDontSee('Motors')
            ->assertSee(route('sources.show', 'ebay-apple'), false)
            ->assertDontSee(route('sources.show', 'ebay-lego'), false)
            ->assertDontSee(route('sources.show', 'ebay-tools'), false);
    }

    public function test_authenticated_users_can_see_a_published_listing_and_its_math(): void
    {
        $user = User::factory()->create();
        PublishedListing::query()->create([
            'external_id' => '123456789012',
            'source' => 'ebay-apple',
            'title' => 'MacBook Air M2 16GB 512GB',
            'url' => 'https://www.ebay.co.uk/itm/123456789012',
            'model_key' => 'apple-macbook-air-m2-16gb-512gb',
            'condition_grade' => 'C',
            'text_condition_grade' => 'B',
            'text_scan_grade' => 'D',
            'text_scan_checked' => true,
            'text_scan_review_required' => true,
            'text_scan_risk_flags' => ['locked_device'],
            'text_scan_model_name' => 'gemma3:12b',
            'text_scan_checked_at' => now()->subMinute(),
            'visual_condition_grade' => 'C',
            'visual_condition_checked' => true,
            'visual_review_required' => true,
            'visual_damage_flags' => ['chipped_corner'],
            'visual_model_name' => 'minicpm-v:8b',
            'visual_checked_at' => now()->subMinute(),
            'sale_type' => 'bin',
            'end_time' => now()->addHour(),
            'current_price' => 350,
            'sold_median' => 500,
            'comp_samples' => 100,
            'max_safe_buy' => 364.20,
            'expected_profit' => 64.20,
            'expected_roi' => 0.1834,
            'price_gap_to_max_buy' => 14.20,
            'effective_min_comps' => 10,
            'signal' => 'confirmed same-grade sold evidence',
            'sync_batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'published_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('dashboard'))
            ->assertOk()
            ->assertDontSee('See the listing.')
            ->assertSee('Source gates')
            ->assertSee('Punt')
            ->assertSee('MacBook Air M2 16GB 512GB')
            ->assertSee('Code B')
            ->assertSee('Text scan D review')
            ->assertSee('Visual C review')
            ->assertSee('Effective C')
            ->assertSee('£64.20')
            ->assertSee('Inspect evidence');
    }

    public function test_dashboard_search_matches_terms_across_listing_fields(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'macbook-m1',
            'source' => 'ebay-apple',
            'title' => 'MacBook Pro 14 inch 16GB 512GB',
            'model_key' => 'apple-macbook-pro-m1-16gb-512gb',
        ]);

        $this->publishListing([
            'external_id' => 'iphone-13',
            'source' => 'ebay-apple',
            'title' => 'Apple iPhone 13 128GB',
            'model_key' => 'apple-iphone-13-128gb',
        ]);

        $this->actingAs($user)
            ->get(route('dashboard', ['search' => 'apple m1']))
            ->assertOk()
            ->assertSee('Try apple m1')
            ->assertSee('MacBook Pro 14 inch 16GB 512GB')
            ->assertDontSee('Apple iPhone 13 128GB');
    }

    public function test_dashboard_filters_potentials_by_sale_type(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'dashboard-auction-row',
            'title' => 'Dashboard auction row',
            'sale_type' => 'auction',
        ]);

        $this->publishListing([
            'external_id' => 'dashboard-bin-row',
            'title' => 'Dashboard BIN row',
            'sale_type' => 'bin',
        ]);

        $this->actingAs($user)
            ->get(route('dashboard'))
            ->assertOk()
            ->assertSee('Both')
            ->assertSee('BIN')
            ->assertSee('Auction')
            ->assertSee('Dashboard BIN row')
            ->assertDontSee('Dashboard auction row');

        $this->actingAs($user)
            ->get(route('dashboard', ['saleType' => 'both']))
            ->assertOk()
            ->assertSee('Dashboard BIN row')
            ->assertSee('Dashboard auction row');

        $this->actingAs($user)
            ->get(route('dashboard', ['saleType' => 'auction']))
            ->assertOk()
            ->assertSee('Dashboard auction row')
            ->assertDontSee('Dashboard BIN row');
    }

    public function test_dashboard_filters_potentials_by_grade(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'dashboard-grade-b-row',
            'title' => 'Dashboard grade B row',
            'condition_grade' => 'B',
        ]);

        $this->publishListing([
            'external_id' => 'dashboard-grade-c-row',
            'title' => 'Dashboard grade C row',
            'condition_grade' => 'C',
        ]);

        $this->actingAs($user)
            ->get(route('dashboard', ['grade' => 'C']))
            ->assertOk()
            ->assertSee('All grades')
            ->assertSee('Grade B')
            ->assertSee('Grade C')
            ->assertSee('Dashboard grade C row')
            ->assertDontSee('Dashboard grade B row');
    }

    public function test_dashboard_filters_potentials_by_visual_check_status(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'dashboard-visual-checked-row',
            'title' => 'Dashboard visually checked row',
            'visual_condition_checked' => true,
            'visual_condition_grade' => 'B',
            'visual_checked_at' => now(),
        ]);

        $this->publishListing([
            'external_id' => 'dashboard-visual-unchecked-row',
            'title' => 'Dashboard visually unchecked row',
            'visual_condition_checked' => false,
        ]);

        $this->actingAs($user)
            ->get(route('dashboard', ['visual' => 'checked']))
            ->assertOk()
            ->assertSee('Visual')
            ->assertSee('Checked')
            ->assertSee('Unchecked')
            ->assertSee('Dashboard visually checked row')
            ->assertDontSee('Dashboard visually unchecked row');

        $this->actingAs($user)
            ->get(route('dashboard', ['visual' => 'unchecked']))
            ->assertOk()
            ->assertSee('Dashboard visually unchecked row')
            ->assertDontSee('Dashboard visually checked row');
    }

    public function test_dashboard_filters_potentials_by_text_scan_status(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'dashboard-text-scan-done-row',
            'title' => 'Dashboard text scan done row',
            'text_scan_checked' => true,
            'text_scan_grade' => 'B',
            'text_scan_checked_at' => now(),
        ]);

        $this->publishListing([
            'external_id' => 'dashboard-text-scan-pending-row',
            'title' => 'Dashboard text scan pending row',
            'text_scan_checked' => false,
        ]);

        $this->actingAs($user)
            ->get(route('dashboard', ['textScan' => 'checked']))
            ->assertOk()
            ->assertSee('Text scan')
            ->assertSee('Checked')
            ->assertSee('Unchecked')
            ->assertSee('Dashboard text scan done row')
            ->assertDontSee('Dashboard text scan pending row');

        $this->actingAs($user)
            ->get(route('dashboard', ['textScan' => 'unchecked']))
            ->assertOk()
            ->assertSee('Dashboard text scan pending row')
            ->assertDontSee('Dashboard text scan done row');
    }

    public function test_dashboard_prioritises_visually_checked_potentials_by_default(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'unchecked-earlier-profit-row',
            'title' => 'Unchecked earlier high profit row',
            'end_time' => now()->addMinutes(20),
            'expected_profit' => 120,
            'expected_roi' => 0.8,
            'price_gap_to_max_buy' => 90,
            'visual_condition_checked' => false,
        ]);

        $this->publishListing([
            'external_id' => 'checked-later-profit-row',
            'title' => 'Checked later solid profit row',
            'end_time' => now()->addHours(4),
            'expected_profit' => 55,
            'expected_roi' => 0.3,
            'price_gap_to_max_buy' => 20,
            'visual_condition_checked' => true,
            'visual_condition_grade' => 'B',
            'visual_review_required' => false,
            'visual_checked_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('dashboard'))
            ->assertOk()
            ->assertSee('Priority')
            ->assertSee('Ending')
            ->assertSee('Profit')
            ->assertSeeInOrder([
                'Checked later solid profit row',
                'Unchecked earlier high profit row',
            ]);

        $this->actingAs($user)
            ->get(route('dashboard', ['sort' => 'ending']))
            ->assertOk()
            ->assertSeeInOrder([
                'Unchecked earlier high profit row',
                'Checked later solid profit row',
            ]);
    }

    public function test_search_matches_normalized_model_keys(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'macbook-a2442',
            'source' => 'ebay-apple',
            'title' => 'Apple MacBook Pro 14 inch M1 Pro',
            'model_key' => 'apple-macbook-pro-a2442-14in-m1pro-16gb-512gb',
        ]);

        $this->publishListing([
            'external_id' => 'macbook-other',
            'source' => 'ebay-apple',
            'title' => 'Apple MacBook Air M1',
            'model_key' => 'apple-macbook-air-a2337-13in-m1-8gb-256gb',
        ]);

        $this->actingAs($user)
            ->get(route('dashboard', ['search' => 'applemacbookproa244214inm1pro16gb512gb']))
            ->assertOk()
            ->assertSee('Apple MacBook Pro 14 inch M1 Pro')
            ->assertDontSee('Apple MacBook Air M1');

        $this->actingAs($user)
            ->get(route('search.index', ['search' => 'a2442 m1pro 16GB/512GB']))
            ->assertOk()
            ->assertSee('Apple MacBook Pro 14 inch M1 Pro')
            ->assertDontSee('Apple MacBook Air M1');
    }

    public function test_global_search_shows_model_key_market_evidence(): void
    {
        $user = User::factory()->create();

        PublishedModelKeyComp::query()->create([
            'source' => 'ebay-apple',
            'model_key' => 'apple-macbook-pro-a2442-14in-m1pro-16gb-512gb',
            'condition_grade' => 'B',
            'samples' => 89,
            'median_final_price' => 639.99,
            'computed_at' => now()->subHour(),
            'synced_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('search.index', ['search' => 'a2442 m1pro 16GB/512GB']))
            ->assertOk()
            ->assertSee('Market evidence')
            ->assertSee('apple-macbook-pro-a2442-14in-m1pro-16gb-512gb')
            ->assertSee('£639.99')
            ->assertSee('89');
    }

    public function test_model_key_market_evidence_page_shows_underlying_comps_and_prices(): void
    {
        $user = User::factory()->create();

        $summary = PublishedModelKeyComp::query()->create([
            'source' => 'ebay-apple',
            'model_key' => 'apple-macbook-pro-a2442-14in-m1pro-16gb-512gb',
            'condition_grade' => 'B',
            'samples' => 89,
            'median_final_price' => 639.99,
            'computed_at' => now()->subHour(),
            'synced_at' => now(),
        ]);

        PublishedModelKeyCompRow::query()->create([
            'published_model_key_comp_id' => $summary->id,
            'comp_external_id' => '157855519619',
            'source' => 'ebay-apple',
            'title' => 'Apple MacBook Pro 14" A2442 2021 M1 Pro 16GB 512GB',
            'url' => 'https://www.ebay.co.uk/itm/157855519619',
            'sale_type' => 'bin',
            'end_time' => now()->subDay(),
            'final_price' => 639.99,
            'bids_count' => 0,
            'visual_condition_grade' => 'B',
            'visual_condition_checked' => true,
            'visual_review_required' => false,
            'visual_checked_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('model-key-comps.show', $summary))
            ->assertOk()
            ->assertSee('Market evidence')
            ->assertSee('Suggested BIN')
            ->assertSee('£608')
            ->assertSee('Max safe buy')
            ->assertSee('£489.79')
            ->assertSee('Visual B')
            ->assertSee('Apple MacBook Pro 14&quot; A2442 2021 M1 Pro 16GB 512GB', false);
    }

    public function test_source_page_search_matches_terms_across_listing_fields(): void
    {
        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'source-macbook-m1',
            'source' => 'ebay-apple',
            'title' => 'MacBook Air 13 inch 8GB 256GB',
            'model_key' => 'apple-macbook-air-m1-8gb-256gb',
            'end_time' => now()->addHour(),
        ]);

        $this->publishListing([
            'external_id' => 'source-iphone',
            'source' => 'ebay-apple',
            'title' => 'Apple iPhone 14 128GB',
            'model_key' => 'apple-iphone-14-128gb',
            'end_time' => now()->addMinutes(10),
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'search' => 'apple m1']))
            ->assertOk()
            ->assertSee('Search Apple')
            ->assertSee('MacBook Air 13 inch 8GB 256GB')
            ->assertDontSee('Apple iPhone 14 128GB');
    }

    public function test_global_search_finds_rows_outside_buy_desk_filters(): void
    {
        config([
            'services.ghostfrog.buy_signal_min_comps' => 100,
            'services.ghostfrog.hidden_sources' => ['ebay-motors'],
        ]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'search-clears-gate',
            'source' => 'ebay-apple',
            'title' => 'MacBook Pro M1 clearing buy row',
            'model_key' => 'apple-macbook-pro-m1-16gb-512gb',
            'current_price' => 300,
            'sold_median' => 520,
            'comp_samples' => 100,
            'max_safe_buy' => 380,
            'expected_profit' => 80,
        ]);

        $this->publishListing([
            'external_id' => 'search-too-expensive',
            'source' => 'ebay-apple',
            'title' => 'MacBook Pro M1 currently too expensive',
            'model_key' => 'apple-macbook-pro-m1-16gb-512gb',
            'current_price' => 499,
            'sold_median' => 550,
            'comp_samples' => 10,
            'max_safe_buy' => 411,
            'expected_profit' => -38,
        ]);

        $this->publishListing([
            'external_id' => 'search-low-evidence',
            'source' => 'ebay-apple',
            'title' => 'MacBook Pro M1 thin evidence but clears',
            'model_key' => 'apple-macbook-pro-m1-16gb-512gb',
            'current_price' => 300,
            'sold_median' => 520,
            'comp_samples' => 3,
            'max_safe_buy' => 380,
            'expected_profit' => 80,
        ]);

        $this->publishListing([
            'external_id' => 'search-no-comps',
            'source' => 'ebay-apple',
            'title' => 'MacBook Pro M1 no comps yet',
            'model_key' => 'apple-macbook-pro-m1-unknown',
            'sold_median' => 0,
            'comp_samples' => 0,
            'max_safe_buy' => 0,
            'expected_profit' => 0,
        ]);

        $this->publishListing([
            'external_id' => 'search-hidden-source',
            'source' => 'ebay-motors',
            'title' => 'MacBook Pro M1 hidden source row',
            'model_key' => 'apple-macbook-pro-m1-hidden',
            'comp_samples' => 100,
            'max_safe_buy' => 380,
            'expected_profit' => 80,
        ]);

        PublishedSoldEvidence::query()->create([
            'external_id' => 'search-sold-comp',
            'source' => 'ebay-apple',
            'title' => 'Sold MacBook Pro M1 comp',
            'url' => 'https://www.ebay.co.uk/itm/search-sold-comp',
            'sale_type' => 'bin',
            'end_time' => now()->subDay(),
            'final_price' => 520,
            'bids_count' => 12,
            'condition_grade' => 'B',
            'model_key' => 'apple-macbook-pro-m1-16gb-512gb',
            'visual_condition_grade' => 'C',
            'visual_condition_checked' => true,
            'visual_review_required' => true,
            'visual_checked_at' => now(),
            'synced_at' => now(),
        ]);

        ListingFavourite::query()->create([
            'user_id' => $user->id,
            'external_id' => 'saved-search-favourite',
            'source' => 'ebay-apple',
            'title' => 'Saved MacBook Pro M1 favourite',
            'url' => 'https://www.ebay.co.uk/itm/saved-search-favourite',
            'model_key' => 'apple-macbook-pro-m1-16gb-512gb',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addHour(),
            'current_price' => 310,
            'sold_median' => 520,
            'comp_samples' => 100,
            'max_safe_buy' => 380,
            'expected_profit' => 70,
            'expected_roi' => 0.2258,
            'price_gap_to_max_buy' => 70,
            'effective_min_comps' => 10,
            'signal' => 'saved search favourite',
            'favourited_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('search.index', ['search' => 'macbook pro']))
            ->assertOk()
            ->assertSee('Find anything we know')
            ->assertSee('MacBook Pro M1 clearing buy row')
            ->assertSee('Clears gate')
            ->assertSee('MacBook Pro M1 currently too expensive')
            ->assertSee('Too expensive')
            ->assertSee('MacBook Pro M1 thin evidence but clears')
            ->assertSee('Low evidence')
            ->assertSee('MacBook Pro M1 no comps yet')
            ->assertSee('No comps')
            ->assertSee('MacBook Pro M1 hidden source row')
            ->assertSee('Hidden source')
            ->assertSee('Sold MacBook Pro M1 comp')
            ->assertSee('Sold evidence')
            ->assertSee('Visual C review')
            ->assertSee('Saved MacBook Pro M1 favourite');
    }

    public function test_users_can_favourite_live_listings_and_keep_saved_evidence_after_sync_removes_them(): void
    {
        $user = User::factory()->create();
        $otherUser = User::factory()->create();

        $listing = $this->publishListing([
            'external_id' => 'favourite-listing',
            'source' => 'ebay-apple',
            'title' => 'Favourite MacBook Air M1',
            'model_key' => 'apple-macbook-air-m1-8gb-256gb',
            'current_price' => 250,
            'sold_median' => 420,
            'comp_samples' => 100,
            'max_safe_buy' => 300,
            'expected_profit' => 50,
            'text_condition_grade' => 'B',
            'visual_condition_grade' => 'C',
            'visual_condition_checked' => true,
            'visual_review_required' => true,
            'visual_checked_at' => now(),
        ]);

        PublishedComp::query()->create([
            'listing_external_id' => $listing->external_id,
            'comp_external_id' => 'saved-comp',
            'source' => 'ebay-apple',
            'title' => 'Sold MacBook Air M1 comp',
            'url' => 'https://www.ebay.co.uk/itm/saved-comp',
            'sale_type' => 'bin',
            'end_time' => now()->subDay(),
            'final_price' => 420,
            'bids_count' => 12,
            'condition_grade' => 'B',
            'model_key' => 'apple-macbook-air-m1-8gb-256gb',
            'visual_condition_grade' => 'B',
            'visual_condition_checked' => true,
            'visual_review_required' => false,
            'visual_checked_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('listings.show', $listing))
            ->assertOk()
            ->assertSee('Add favourite')
            ->assertSee('aria-label="Current price"', false)
            ->assertSee('x-model.number="currentPrice"', false)
            ->assertSee('x-text="money(profit())"', false)
            ->assertDontSee('Remove favourite');

        $this->actingAs($user)
            ->post(route('listings.favourites.store', $listing))
            ->assertRedirect();

        $favourite = ListingFavourite::query()
            ->where('user_id', $user->id)
            ->where('external_id', $listing->external_id)
            ->sole();

        $this->assertCount(1, $favourite->comps_snapshot);
        $this->assertTrue((bool) $favourite->visual_condition_checked);
        $this->assertSame('C', $favourite->visual_condition_grade);
        $this->assertTrue((bool) $favourite->visual_review_required);
        $this->assertTrue((bool) $favourite->comps_snapshot[0]['visual_condition_checked']);
        $this->assertSame('B', $favourite->comps_snapshot[0]['visual_condition_grade']);

        $this->actingAs($user)
            ->get(route('listings.show', $listing))
            ->assertOk()
            ->assertSee('Remove favourite')
            ->assertDontSee('Add favourite');

        $this->actingAs($user)
            ->get(route('dashboard'))
            ->assertOk()
            ->assertSee('Saved listings')
            ->assertSee('(1)', false)
            ->assertSee('Remove favourite');

        $this->actingAs($otherUser)
            ->get(route('favourites.index'))
            ->assertOk()
            ->assertSee('No favourites yet')
            ->assertDontSee('Favourite MacBook Air M1');

        $listing->delete();

        $this->actingAs($user)
            ->get(route('favourites.index'))
            ->assertOk()
            ->assertSee('Favourite MacBook Air M1')
            ->assertSee('Saved snapshot')
            ->assertSee('Visual C review');

        $this->actingAs($user)
            ->get(route('favourites.show', $favourite))
            ->assertOk()
            ->assertSee('Favourite MacBook Air M1')
            ->assertSee('Sold MacBook Air M1 comp')
            ->assertSee('Visual C review')
            ->assertSee('Visual B')
            ->assertSee('£420.00');

        $this->actingAs($user)
            ->delete(route('favourites.destroy', $favourite))
            ->assertRedirect();

        $this->assertDatabaseMissing('listing_favourites', [
            'id' => $favourite->id,
        ]);
    }

    public function test_dashboard_uses_email_evidence_floor_and_can_sort_by_ending_soonest(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();
        $syncBatchId = 'c857fb4e-8d46-46f0-9376-18f60870a8c7';

        PublishedListing::query()->create([
            'external_id' => 'thin-evidence',
            'source' => 'ebay-apple',
            'title' => 'Thin evidence listing',
            'url' => 'https://www.ebay.co.uk/itm/thin-evidence',
            'model_key' => 'apple-thin-evidence',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addMinutes(10),
            'current_price' => 100,
            'sold_median' => 220,
            'comp_samples' => 13,
            'max_safe_buy' => 160,
            'expected_profit' => 50,
            'expected_roi' => 0.5,
            'price_gap_to_max_buy' => 60,
            'effective_min_comps' => 10,
            'signal' => 'thin evidence should not reach buy desk',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        PublishedListing::query()->create([
            'external_id' => 'ending-later',
            'source' => 'ebay-apple',
            'title' => 'Enough evidence ending later',
            'url' => 'https://www.ebay.co.uk/itm/ending-later',
            'model_key' => 'apple-ending-later',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addDays(2),
            'current_price' => 100,
            'sold_median' => 260,
            'comp_samples' => 100,
            'max_safe_buy' => 180,
            'expected_profit' => 70,
            'expected_roi' => 0.7,
            'price_gap_to_max_buy' => 80,
            'effective_min_comps' => 10,
            'signal' => 'enough evidence later',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        PublishedListing::query()->create([
            'external_id' => 'ending-soon',
            'source' => 'ebay-apple',
            'title' => 'Enough evidence ending soon',
            'url' => 'https://www.ebay.co.uk/itm/ending-soon',
            'model_key' => 'apple-ending-soon',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addHour(),
            'current_price' => 100,
            'sold_median' => 230,
            'comp_samples' => 100,
            'max_safe_buy' => 170,
            'expected_profit' => 55,
            'expected_roi' => 0.55,
            'price_gap_to_max_buy' => 70,
            'effective_min_comps' => 10,
            'signal' => 'enough evidence soon',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('dashboard', ['sort' => 'ending']))
            ->assertOk()
            ->assertDontSee('Thin evidence listing')
            ->assertSeeInOrder([
                'Enough evidence ending soon',
                'Enough evidence ending later',
            ]);
    }

    public function test_punt_band_excludes_serious_profit_rows(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();
        $syncBatchId = 'c857fb4e-8d46-46f0-9376-18f60870a8c7';

        PublishedListing::query()->create([
            'external_id' => 'serious-row',
            'source' => 'ebay-apple',
            'title' => 'Serious profit listing',
            'url' => 'https://www.ebay.co.uk/itm/serious-row',
            'model_key' => 'apple-serious-row',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addHour(),
            'current_price' => 411,
            'sold_median' => 600,
            'comp_samples' => 143,
            'max_safe_buy' => 455,
            'expected_profit' => 94,
            'expected_roi' => 0.23,
            'price_gap_to_max_buy' => 44,
            'effective_min_comps' => 10,
            'signal' => 'serious row should not appear in punt band',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        PublishedListing::query()->create([
            'external_id' => 'punt-row',
            'source' => 'ebay-apple',
            'title' => 'Punt profit listing',
            'url' => 'https://www.ebay.co.uk/itm/punt-row',
            'model_key' => 'apple-punt-row',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addHours(2),
            'current_price' => 180,
            'sold_median' => 280,
            'comp_samples' => 120,
            'max_safe_buy' => 225,
            'expected_profit' => 35,
            'expected_roi' => 0.19,
            'price_gap_to_max_buy' => 45,
            'effective_min_comps' => 10,
            'signal' => 'punt row should appear in punt band',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('dashboard', ['profit' => 25]))
            ->assertOk()
            ->assertDontSee('Serious profit listing')
            ->assertSee('Punt profit listing');
    }

    public function test_dashboard_default_gate_uses_lower_console_profit_threshold(): void
    {
        config(['services.ghostfrog.buy_signal_min_comps' => 100]);

        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'console-twenty-pound-gate-row',
            'source' => 'ebay-consoles',
            'title' => 'Console twenty pound gate row',
            'current_price' => 63,
            'sold_median' => 119.99,
            'comp_samples' => 125,
            'max_safe_buy' => 37.39,
            'expected_profit' => 24.39,
            'expected_roi' => 0.39,
            'price_gap_to_max_buy' => -25.61,
        ]);

        $this->publishListing([
            'external_id' => 'samsung-twenty-pound-row',
            'source' => 'ebay-samsung',
            'title' => 'Samsung twenty pound row',
            'current_price' => 63,
            'sold_median' => 119.99,
            'comp_samples' => 125,
            'max_safe_buy' => 37.39,
            'expected_profit' => 24.39,
            'expected_roi' => 0.39,
            'price_gap_to_max_buy' => -25.61,
        ]);

        $this->actingAs($user)
            ->get(route('dashboard'))
            ->assertOk()
            ->assertSee('Source gates')
            ->assertSee('Console twenty pound gate row')
            ->assertDontSee('Samsung twenty pound row');
    }

    public function test_source_page_lists_comped_source_rows_by_ending_soonest(): void
    {
        $user = User::factory()->create();
        $syncBatchId = 'c857fb4e-8d46-46f0-9376-18f60870a8c7';

        PublishedListing::query()->create([
            'external_id' => 'apple-later',
            'source' => 'ebay-apple',
            'title' => 'Apple row ending later',
            'url' => 'https://www.ebay.co.uk/itm/apple-later',
            'model_key' => 'apple-later',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addDays(2),
            'current_price' => 100,
            'sold_median' => 150,
            'comp_samples' => 1,
            'max_safe_buy' => 120,
            'expected_profit' => 70,
            'expected_roi' => 0.7,
            'price_gap_to_max_buy' => 20,
            'effective_min_comps' => 10,
            'signal' => 'source page includes thin rows that clear the gate',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        PublishedListing::query()->create([
            'external_id' => 'apple-soon',
            'source' => 'ebay-apple',
            'title' => 'Apple row ending soon',
            'url' => 'https://www.ebay.co.uk/itm/apple-soon',
            'model_key' => 'apple-soon',
            'condition_grade' => 'C',
            'sale_type' => 'bin',
            'end_time' => now()->addHour(),
            'current_price' => 50,
            'sold_median' => 90,
            'comp_samples' => 0,
            'max_safe_buy' => 40,
            'expected_profit' => -20,
            'expected_roi' => -0.4,
            'price_gap_to_max_buy' => -10,
            'effective_min_comps' => 10,
            'signal' => 'source page includes rows with no comps',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        PublishedListing::query()->create([
            'external_id' => 'apple-too-expensive',
            'source' => 'ebay-apple',
            'title' => 'Apple row too expensive',
            'url' => 'https://www.ebay.co.uk/itm/apple-too-expensive',
            'model_key' => 'apple-too-expensive',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addMinutes(20),
            'current_price' => 140,
            'sold_median' => 150,
            'comp_samples' => 1,
            'max_safe_buy' => 90,
            'expected_profit' => -10,
            'expected_roi' => -0.07,
            'price_gap_to_max_buy' => -50,
            'effective_min_comps' => 10,
            'signal' => 'source page all mode can include rows that miss the gate',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        PublishedListing::query()->create([
            'external_id' => 'apple-earliest-clear',
            'source' => 'ebay-apple',
            'title' => 'Apple row ending earliest and clearing',
            'url' => 'https://www.ebay.co.uk/itm/apple-earliest-clear',
            'model_key' => 'apple-earliest-clear',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addMinutes(40),
            'current_price' => 80,
            'sold_median' => 150,
            'comp_samples' => 1,
            'max_safe_buy' => 90,
            'expected_profit' => 60,
            'expected_roi' => 0.75,
            'price_gap_to_max_buy' => 10,
            'effective_min_comps' => 10,
            'signal' => 'source page orders clearing rows by ending soonest',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        PublishedListing::query()->create([
            'external_id' => 'samsung-row',
            'source' => 'ebay-samsung',
            'title' => 'Samsung row',
            'url' => 'https://www.ebay.co.uk/itm/samsung-row',
            'model_key' => 'samsung-row',
            'condition_grade' => 'B',
            'sale_type' => 'auction',
            'end_time' => now()->addMinutes(30),
            'current_price' => 70,
            'sold_median' => 120,
            'comp_samples' => 100,
            'max_safe_buy' => 80,
            'expected_profit' => 20,
            'expected_roi' => 0.28,
            'price_gap_to_max_buy' => 10,
            'effective_min_comps' => 5,
            'signal' => 'other source',
            'sync_batch_id' => $syncBatchId,
            'published_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', 'ebay-apple'))
            ->assertOk()
            ->assertSee('Showing rows with expected profit of at least £50')
            ->assertSee('Clears £50')
            ->assertSee('All')
            ->assertSee('Listing')
            ->assertDontSee('Samsung row')
            ->assertDontSee('Apple row ending soon')
            ->assertDontSee('Apple row too expensive')
            ->assertSeeInOrder([
                'Apple row ending earliest and clearing',
                'Apple row ending later',
            ]);

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'gate' => 'all']))
            ->assertOk()
            ->assertSee('Rows without sold evidence or a safe buy price are hidden')
            ->assertSee('Apple row too expensive');
    }

    public function test_console_source_page_uses_twenty_pound_gate(): void
    {
        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'console-source-twenty-four-profit',
            'source' => 'ebay-consoles',
            'title' => 'Console source twenty four profit',
            'current_price' => 63,
            'sold_median' => 119.99,
            'comp_samples' => 125,
            'max_safe_buy' => 37.39,
            'expected_profit' => 24.39,
            'expected_roi' => 0.39,
            'price_gap_to_max_buy' => -25.61,
        ]);

        $this->publishListing([
            'external_id' => 'console-source-nineteen-profit',
            'source' => 'ebay-consoles',
            'title' => 'Console source nineteen profit',
            'current_price' => 68,
            'sold_median' => 119.99,
            'comp_samples' => 125,
            'max_safe_buy' => 37.39,
            'expected_profit' => 19.39,
            'expected_roi' => 0.28,
            'price_gap_to_max_buy' => -30.61,
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', 'ebay-consoles'))
            ->assertOk()
            ->assertSee('Showing rows with expected profit of at least £20')
            ->assertSee('Clears £20')
            ->assertSee('Console source twenty four profit')
            ->assertDontSee('Console source nineteen profit');
    }

    public function test_source_page_filters_listings_by_sale_type(): void
    {
        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'source-auction-row',
            'source' => 'ebay-apple',
            'title' => 'Source auction row',
            'sale_type' => 'auction',
            'comp_samples' => 1,
        ]);

        $this->publishListing([
            'external_id' => 'source-bin-row',
            'source' => 'ebay-apple',
            'title' => 'Source BIN row',
            'sale_type' => 'bin',
            'comp_samples' => 1,
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple']))
            ->assertOk()
            ->assertSee('Both')
            ->assertSee('BIN')
            ->assertSee('Auction')
            ->assertSee('Source BIN row')
            ->assertDontSee('Source auction row');

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'saleType' => 'both']))
            ->assertOk()
            ->assertSee('Source BIN row')
            ->assertSee('Source auction row');

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'saleType' => 'auction']))
            ->assertOk()
            ->assertSee('Source auction row')
            ->assertDontSee('Source BIN row');
    }

    public function test_source_page_filters_listings_by_grade(): void
    {
        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'source-grade-b-row',
            'source' => 'ebay-apple',
            'title' => 'Source grade B row',
            'condition_grade' => 'B',
            'comp_samples' => 1,
        ]);

        $this->publishListing([
            'external_id' => 'source-grade-c-row',
            'source' => 'ebay-apple',
            'title' => 'Source grade C row',
            'condition_grade' => 'C',
            'comp_samples' => 1,
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'grade' => 'C']))
            ->assertOk()
            ->assertSee('All grades')
            ->assertSee('Grade B')
            ->assertSee('Grade C')
            ->assertSee('Source grade C row')
            ->assertDontSee('Source grade B row');
    }

    public function test_source_page_filters_listings_by_visual_check_status(): void
    {
        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'source-visual-checked-row',
            'source' => 'ebay-apple',
            'title' => 'Source visually checked row',
            'comp_samples' => 1,
            'visual_condition_checked' => true,
            'visual_condition_grade' => 'C',
            'visual_review_required' => true,
            'visual_checked_at' => now(),
        ]);

        $this->publishListing([
            'external_id' => 'source-visual-unchecked-row',
            'source' => 'ebay-apple',
            'title' => 'Source visually unchecked row',
            'comp_samples' => 1,
            'visual_condition_checked' => false,
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'visual' => 'checked']))
            ->assertOk()
            ->assertSee('Visual')
            ->assertSee('Checked')
            ->assertSee('Unchecked')
            ->assertSee('Source visually checked row')
            ->assertDontSee('Source visually unchecked row');

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'visual' => 'unchecked']))
            ->assertOk()
            ->assertSee('Source visually unchecked row')
            ->assertDontSee('Source visually checked row');
    }

    public function test_source_page_filters_listings_by_text_scan_status(): void
    {
        $user = User::factory()->create();

        $this->publishListing([
            'external_id' => 'source-text-scan-done-row',
            'source' => 'ebay-apple',
            'title' => 'Source text scan done row',
            'comp_samples' => 1,
            'text_scan_checked' => true,
            'text_scan_grade' => 'C',
            'text_scan_review_required' => true,
            'text_scan_checked_at' => now(),
        ]);

        $this->publishListing([
            'external_id' => 'source-text-scan-pending-row',
            'source' => 'ebay-apple',
            'title' => 'Source text scan pending row',
            'comp_samples' => 1,
            'text_scan_checked' => false,
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'textScan' => 'checked']))
            ->assertOk()
            ->assertSee('Text scan')
            ->assertSee('Checked')
            ->assertSee('Unchecked')
            ->assertSee('Source text scan done row')
            ->assertDontSee('Source text scan pending row');

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'ebay-apple', 'textScan' => 'unchecked']))
            ->assertOk()
            ->assertSee('Source text scan pending row')
            ->assertDontSee('Source text scan done row');
    }

    public function test_motomine_source_page_can_show_rows_without_comps(): void
    {
        $user = User::factory()->create();

        PublishedListing::query()->create([
            'external_id' => 'motomine-no-comps',
            'source' => 'motomine',
            'title' => 'Motomine row without comps',
            'url' => 'https://www.ebay.co.uk/itm/motomine-no-comps',
            'model_key' => 'motomine-row',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addHour(),
            'current_price' => 100,
            'sold_median' => 0,
            'comp_samples' => 0,
            'max_safe_buy' => 0,
            'expected_profit' => 0,
            'expected_roi' => 0,
            'price_gap_to_max_buy' => 0,
            'effective_min_comps' => 1,
            'signal' => 'motomine browse row',
            'sync_batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'published_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', ['source' => 'motomine', 'gate' => 'all']))
            ->assertOk()
            ->assertSee('Motomine can show rows before sold evidence is attached')
            ->assertSee('Motomine row without comps');
    }

    public function test_source_pages_hide_negative_max_safe_buy_rows(): void
    {
        $user = User::factory()->create();

        PublishedListing::query()->create([
            'external_id' => 'negative-max-buy',
            'source' => 'ebay-pokemon',
            'title' => '50x Pokemon Card Bundle',
            'url' => 'https://www.ebay.co.uk/itm/negative-max-buy',
            'model_key' => 'pokemon-pikachu-holo-en',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addHour(),
            'current_price' => 5,
            'sold_median' => 5.99,
            'comp_samples' => 1,
            'max_safe_buy' => -61.79,
            'expected_profit' => -16.79,
            'expected_roi' => -3.358,
            'price_gap_to_max_buy' => -66.79,
            'effective_min_comps' => 15,
            'signal' => 'not viable after fixed costs',
            'sync_batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'published_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('sources.show', 'ebay-pokemon'))
            ->assertOk()
            ->assertDontSee('50x Pokemon Card Bundle')
            ->assertSee('No listings for this source yet.');
    }

    public function test_hidden_vehicle_sources_do_not_show_in_navigation_or_source_pages(): void
    {
        $user = User::factory()->create();

        PublishedListing::query()->create([
            'external_id' => 'hidden-motor',
            'source' => 'ebay-motors',
            'title' => 'Hidden motor row',
            'url' => 'https://www.ebay.co.uk/itm/hidden-motor',
            'model_key' => 'hidden-motor-row',
            'condition_grade' => 'B',
            'sale_type' => 'auction',
            'end_time' => now()->addHour(),
            'current_price' => 100,
            'sold_median' => 200,
            'comp_samples' => 10,
            'max_safe_buy' => 120,
            'expected_profit' => 40,
            'expected_roi' => 0.4,
            'price_gap_to_max_buy' => 20,
            'effective_min_comps' => 1,
            'signal' => 'hidden source',
            'sync_batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'published_at' => now(),
        ]);

        $this->actingAs($user)
            ->get(route('dashboard'))
            ->assertOk()
            ->assertDontSee('Motors')
            ->assertDontSee('Hidden motor row');

        $this->actingAs($user)
            ->get(route('sources.show', 'ebay-motors'))
            ->assertOk()
            ->assertDontSee('Hidden motor row')
            ->assertSee('No listings for this source yet.');
    }

    /**
     * @param  array<string, mixed>  $overrides
     */
    private function publishListing(array $overrides): PublishedListing
    {
        return PublishedListing::query()->create([
            'external_id' => 'test-listing',
            'source' => 'ebay-apple',
            'title' => 'Test listing',
            'url' => 'https://www.ebay.co.uk/itm/test-listing',
            'model_key' => 'test-listing',
            'condition_grade' => 'B',
            'sale_type' => 'bin',
            'end_time' => now()->addHour(),
            'current_price' => 100,
            'sold_median' => 220,
            'comp_samples' => 100,
            'max_safe_buy' => 160,
            'expected_profit' => 60,
            'expected_roi' => 0.6,
            'price_gap_to_max_buy' => 60,
            'effective_min_comps' => 10,
            'signal' => 'test row',
            'sync_batch_id' => 'c857fb4e-8d46-46f0-9376-18f60870a8c7',
            'published_at' => now(),
            ...$overrides,
        ]);
    }
}
