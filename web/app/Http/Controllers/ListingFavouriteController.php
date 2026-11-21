<?php

namespace App\Http\Controllers;

use App\Models\ListingFavourite;
use App\Models\PublishedListing;
use Illuminate\Http\RedirectResponse;
use Illuminate\Http\Request;

class ListingFavouriteController extends Controller
{
    public function store(Request $request, PublishedListing $listing): RedirectResponse
    {
        $user = $request->user();

        abort_if($user === null, 403);

        ListingFavourite::query()->updateOrCreate(
            [
                'user_id' => $user->id,
                'external_id' => $listing->external_id,
            ],
            [
                'source' => $listing->source,
                'title' => $listing->title,
                'url' => $listing->url,
                'model_key' => $listing->model_key,
                'condition_grade' => $listing->condition_grade,
                'text_condition_grade' => $listing->text_condition_grade,
                'text_scan_grade' => $listing->text_scan_grade,
                'text_scan_checked' => $listing->text_scan_checked,
                'text_scan_review_required' => $listing->text_scan_review_required,
                'text_scan_risk_flags' => $listing->text_scan_risk_flags,
                'text_scan_model_name' => $listing->text_scan_model_name,
                'text_scan_checked_at' => $listing->text_scan_checked_at,
                'visual_condition_grade' => $listing->visual_condition_grade,
                'visual_condition_checked' => $listing->visual_condition_checked,
                'visual_review_required' => $listing->visual_review_required,
                'visual_damage_flags' => $listing->visual_damage_flags,
                'visual_model_name' => $listing->visual_model_name,
                'visual_checked_at' => $listing->visual_checked_at,
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
                'comps_snapshot' => $this->compsSnapshot($listing),
                'favourited_at' => now(),
            ],
        );

        return back()->with('status', 'Added to favourites.');
    }

    public function destroy(Request $request, ListingFavourite $favourite): RedirectResponse
    {
        abort_if($favourite->user_id !== $request->user()?->id, 404);

        $favourite->delete();

        return back()->with('status', 'Removed from favourites.');
    }

    /**
     * @return list<array<string, mixed>>
     */
    private function compsSnapshot(PublishedListing $listing): array
    {
        $snapshot = [];

        foreach ($listing->comps()
            ->orderByDesc('visual_condition_checked')
            ->orderBy('visual_review_required')
            ->orderByDesc('end_time')
            ->get() as $comp) {
            $snapshot[] = [
                'comp_external_id' => $comp->comp_external_id,
                'source' => $comp->source,
                'title' => $comp->title,
                'url' => $comp->url,
                'sale_type' => $comp->sale_type,
                'end_time' => $comp->end_time?->toISOString(),
                'final_price' => (float) $comp->final_price,
                'bids_count' => $comp->bids_count,
                'condition_grade' => $comp->condition_grade,
                'model_key' => $comp->model_key,
                'text_scan_grade' => $comp->text_scan_grade,
                'text_scan_checked' => (bool) $comp->text_scan_checked,
                'text_scan_review_required' => (bool) $comp->text_scan_review_required,
                'text_scan_risk_flags' => $comp->text_scan_risk_flags ?? [],
                'text_scan_model_name' => $comp->text_scan_model_name,
                'text_scan_checked_at' => $comp->text_scan_checked_at?->toISOString(),
                'visual_condition_grade' => $comp->visual_condition_grade,
                'visual_condition_checked' => (bool) $comp->visual_condition_checked,
                'visual_review_required' => (bool) $comp->visual_review_required,
                'visual_damage_flags' => $comp->visual_damage_flags ?? [],
                'visual_model_name' => $comp->visual_model_name,
                'visual_checked_at' => $comp->visual_checked_at?->toISOString(),
            ];
        }

        return $snapshot;
    }
}
