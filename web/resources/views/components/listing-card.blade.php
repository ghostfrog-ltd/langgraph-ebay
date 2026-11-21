@props(['listing', 'near' => false, 'mode' => 'opportunity', 'profitGate' => null])

@php
    $isSourceList = $mode === 'source';
    $isSearchList = $mode === 'search';
    $hasEvidence = (int) $listing->comp_samples > 0 && (float) $listing->sold_median > 0;
    $expectedProfit = (float) $listing->expected_profit;
    $profitGate = (float) ($profitGate ?? \App\Support\SourceCatalog::profitGate($listing->source));
    $clearsGate = $hasEvidence && $expectedProfit >= $profitGate && (float) $listing->max_safe_buy >= 0;
    $buySignalMinComps = max(1, (int) config('services.ghostfrog.buy_signal_min_comps', 100));
    $isHiddenSource = \App\Support\SourceCatalog::isHidden($listing->source);
    $favourite = auth()->check()
        ? \App\Models\ListingFavourite::query()
            ->where('user_id', auth()->id())
            ->where('external_id', $listing->external_id)
            ->first()
        : null;
    $codeGrade = $listing->text_condition_grade ?: $listing->condition_grade;
    $effectiveGrade = $listing->condition_grade;
    $textScanGrade = $listing->text_scan_grade;
    $hasTextScan = (bool) $listing->text_scan_checked;
    $textScanNeedsReview = (bool) $listing->text_scan_review_required;
    $visualGrade = $listing->visual_condition_grade;
    $hasVisualCheck = (bool) $listing->visual_condition_checked;
    $visualNeedsReview = (bool) $listing->visual_review_required;

    if ($isSearchList) {
        if ($isHiddenSource) {
            $badgeText = 'Hidden source';
            $badgeClass = 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300';
            $topBarClass = 'bg-zinc-300';
        } elseif (! $hasEvidence) {
            $badgeText = 'No comps';
            $badgeClass = 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300';
            $topBarClass = 'bg-zinc-300';
        } elseif ((float) $listing->max_safe_buy < 0) {
            $badgeText = 'Cost blocked';
            $badgeClass = 'bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300';
            $topBarClass = 'bg-red-400';
        } elseif (! $clearsGate) {
            $badgeText = 'Too expensive';
            $badgeClass = 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300';
            $topBarClass = 'bg-amber-400';
        } elseif ((int) $listing->comp_samples < $buySignalMinComps) {
            $badgeText = 'Low evidence';
            $badgeClass = 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300';
            $topBarClass = 'bg-amber-400';
        } else {
            $badgeText = 'Clears gate';
            $badgeClass = 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300';
            $topBarClass = 'bg-emerald-500';
        }
    } else {
        $badgeClass = $isSourceList
            ? 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300'
            : ($near ? 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300' : 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300');
        $badgeText = $isSourceList ? 'Listing' : ($near ? 'Near miss' : 'Clears gate');
        $topBarClass = $isSourceList ? 'bg-zinc-300' : ($near ? 'bg-amber-400' : 'bg-emerald-500');
    }
@endphp

<article class="group overflow-hidden rounded-2xl border border-zinc-200 bg-white shadow-sm transition hover:-translate-y-0.5 hover:shadow-md dark:border-zinc-700 dark:bg-zinc-900">
    <div class="h-1 {{ $topBarClass }}"></div>
    <div class="p-5 sm:p-6">
        <div class="flex flex-col justify-between gap-5 sm:flex-row">
            <div class="min-w-0 flex-1">
                <div class="mb-2 flex flex-wrap items-center gap-2 text-[11px] font-bold uppercase tracking-wider text-zinc-500">
                    <span class="rounded-full px-2.5 py-1 {{ $badgeClass }}">
                        {{ $badgeText }}
                    </span>
                    <span>{{ \App\Support\SourceCatalog::label($listing->source) }}</span>
                    <span>Code {{ $codeGrade }}</span>
                    <x-text-scan-badge :checked="$hasTextScan" :grade="$textScanGrade" :review="$textScanNeedsReview" compact />
                    <x-visual-badge :checked="$hasVisualCheck" :grade="$visualGrade" :review="$visualNeedsReview" compact />
                    <x-effective-grade-badge :grade="$effectiveGrade" compact />
                </div>
                <a href="{{ route('listings.show', $listing) }}" wire:navigate class="text-lg font-semibold leading-snug text-zinc-950 group-hover:text-emerald-700 dark:text-white dark:group-hover:text-emerald-400">
                    {{ $listing->title }}
                </a>
                <p class="mt-1 truncate font-mono text-[11px] text-zinc-500">{{ $listing->model_key }}</p>
            </div>
            <div class="shrink-0">
                <div class="flex items-start justify-end gap-4 sm:text-right">
                    <div class="min-w-32">
                        @if($isSourceList)
                            <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Expected profit</p>
                            <p class="text-3xl font-semibold tabular-nums {{ $expectedProfit >= $profitGate ? 'text-emerald-700 dark:text-emerald-400' : ($expectedProfit >= 0 ? 'text-amber-700 dark:text-amber-400' : 'text-red-700 dark:text-red-400') }}">£{{ number_format($expectedProfit, 2) }}</p>
                            <p class="text-xs text-zinc-500">{{ $hasEvidence ? number_format((float) $listing->expected_roi * 100).'% ROI' : 'no resale maths yet' }}</p>
                        @elseif($isSearchList)
                            <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Expected profit</p>
                            <p class="text-3xl font-semibold tabular-nums {{ $expectedProfit >= $profitGate ? 'text-emerald-700 dark:text-emerald-400' : ($expectedProfit >= 0 ? 'text-amber-700 dark:text-amber-400' : 'text-red-700 dark:text-red-400') }}">£{{ number_format($expectedProfit, 2) }}</p>
                            <p class="text-xs text-zinc-500">{{ $hasEvidence ? number_format((float) $listing->expected_roi * 100).'% ROI' : 'no resale maths yet' }}</p>
                        @else
                            <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Expected profit</p>
                            <p class="text-3xl font-semibold tabular-nums text-emerald-700 dark:text-emerald-400">£{{ number_format((float) $listing->expected_profit, 2) }}</p>
                            <p class="text-xs text-zinc-500">{{ number_format((float) $listing->expected_roi * 100) }}% ROI</p>
                        @endif
                    </div>
                    <div class="pt-0.5">
                        @if($favourite)
                            <form method="POST" action="{{ route('favourites.destroy', $favourite) }}">
                                @csrf
                                @method('DELETE')
                                <button
                                    type="submit"
                                    aria-label="Remove favourite"
                                    title="Remove favourite"
                                    class="inline-flex size-10 items-center justify-center rounded-full border border-red-200 bg-red-50 text-red-600 transition hover:bg-red-100 hover:text-red-700 dark:border-red-900 dark:bg-red-950/50 dark:text-red-400 dark:hover:bg-red-950"
                                >
                                    <svg class="size-5" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                                        <path d="M11.645 20.91l-.007-.003-.022-.012a15.247 15.247 0 0 1-.383-.218 25.18 25.18 0 0 1-4.244-3.17C4.688 15.36 2.25 12.174 2.25 8.25 2.25 5.322 4.714 3 7.688 3A5.5 5.5 0 0 1 12 5.052 5.5 5.5 0 0 1 16.313 3c2.973 0 5.437 2.322 5.437 5.25 0 3.924-2.438 7.111-4.739 9.256a25.175 25.175 0 0 1-4.244 3.17 15.247 15.247 0 0 1-.383.219l-.022.012-.007.004-.003.001a.752.752 0 0 1-.704 0l-.003-.001Z" />
                                    </svg>
                                    <span class="sr-only">Remove favourite</span>
                                </button>
                            </form>
                        @else
                            <form method="POST" action="{{ route('listings.favourites.store', $listing) }}">
                                @csrf
                                <button
                                    type="submit"
                                    aria-label="Add favourite"
                                    title="Add favourite"
                                    class="inline-flex size-10 items-center justify-center rounded-full border border-zinc-300 bg-white text-zinc-600 transition hover:border-red-200 hover:bg-red-50 hover:text-red-600 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-300 dark:hover:border-red-900 dark:hover:bg-red-950/40 dark:hover:text-red-400"
                                >
                                    <svg class="size-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
                                        <path stroke-linecap="round" stroke-linejoin="round" d="M21 8.25c0-2.485-2.099-4.5-4.688-4.5-1.935 0-3.597 1.126-4.312 2.733C11.285 4.876 9.623 3.75 7.688 3.75 5.099 3.75 3 5.765 3 8.25c0 7.22 9 12 9 12s9-4.78 9-12Z" />
                                    </svg>
                                    <span class="sr-only">Add favourite</span>
                                </button>
                            </form>
                        @endif
                    </div>
                </div>
            </div>
        </div>

        <dl class="mt-5 grid grid-cols-2 divide-x divide-y divide-zinc-200 border-y border-zinc-200 text-sm sm:grid-cols-4 sm:divide-y-0 dark:divide-zinc-700 dark:border-zinc-700">
            <div class="py-3 pr-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Current</dt><dd class="mt-0.5 text-lg font-medium tabular-nums">£{{ number_format((float) $listing->current_price, 2) }}</dd></div>
            @if($isSourceList && ! $hasEvidence)
                <div class="p-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Ends</dt><dd class="mt-0.5 text-sm font-medium">{{ $listing->end_time?->diffForHumans() ?? 'No end time' }}</dd></div>
                <div class="p-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Type</dt><dd class="mt-0.5 text-sm font-medium">{{ str($listing->sale_type)->replace('_', ' ')->title() }}</dd></div>
                <div class="py-3 pl-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Evidence</dt><dd class="mt-0.5 text-sm font-medium">No comps yet</dd></div>
            @else
                <div class="p-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Max safe buy</dt><dd class="mt-0.5 text-lg font-medium tabular-nums">£{{ number_format((float) $listing->max_safe_buy, 2) }}</dd></div>
                <div class="p-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Sold median</dt><dd class="mt-0.5 text-lg font-medium tabular-nums">£{{ number_format((float) $listing->sold_median, 2) }}</dd></div>
                <div class="py-3 pl-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Evidence</dt><dd class="mt-0.5 text-lg font-medium tabular-nums">{{ number_format($listing->comp_samples) }} sold</dd></div>
            @endif
        </dl>

        <div class="mt-4 flex flex-col justify-between gap-3 text-xs text-zinc-500 sm:flex-row sm:items-center">
            <p>{{ $listing->end_time?->isFuture() ? 'Ends '.$listing->end_time->diffForHumans() : 'End time passed' }} · {{ str($listing->sale_type)->replace('_', ' ') }}</p>
            <div class="flex flex-wrap items-center gap-3">
                <a href="{{ $listing->url }}" target="_blank" rel="noreferrer" class="font-medium underline decoration-zinc-300 underline-offset-4">View on eBay ↗</a>
                <flux:button :href="route('listings.show', $listing)" wire:navigate size="sm" variant="primary">{{ $hasEvidence ? 'Inspect evidence' : 'View listing' }}</flux:button>
            </div>
        </div>
    </div>
</article>
