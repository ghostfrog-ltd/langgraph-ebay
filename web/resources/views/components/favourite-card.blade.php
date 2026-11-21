@props(['favourite'])

@php
    $hasEvidence = (int) $favourite->comp_samples > 0 && (float) $favourite->sold_median > 0;
    $isStillLiveInDesk = $favourite->currentListing !== null;
    $effectiveGrade = $favourite->condition_grade ?: $favourite->currentListing?->condition_grade;
    $codeGrade = $favourite->text_condition_grade ?: $favourite->currentListing?->text_condition_grade ?: $effectiveGrade;
    $visualGrade = $favourite->visual_condition_grade ?: $favourite->currentListing?->visual_condition_grade;
    $hasVisualCheck = (bool) ($favourite->visual_condition_checked || $favourite->currentListing?->visual_condition_checked);
    $visualNeedsReview = (bool) ($favourite->visual_review_required || $favourite->currentListing?->visual_review_required);
    $textScanGrade = $favourite->text_scan_grade ?: $favourite->currentListing?->text_scan_grade;
    $hasTextScan = (bool) ($favourite->text_scan_checked || $favourite->currentListing?->text_scan_checked);
    $textScanNeedsReview = (bool) ($favourite->text_scan_review_required || $favourite->currentListing?->text_scan_review_required);
@endphp

<article class="group overflow-hidden rounded-2xl border border-zinc-200 bg-white shadow-sm transition hover:-translate-y-0.5 hover:shadow-md dark:border-zinc-700 dark:bg-zinc-900">
    <div class="h-1 {{ $isStillLiveInDesk ? 'bg-rose-400' : 'bg-zinc-300' }}"></div>
    <div class="p-5 sm:p-6">
        <div class="flex flex-col justify-between gap-5 sm:flex-row">
            <div class="min-w-0 flex-1">
                <div class="mb-2 flex flex-wrap items-center gap-2 text-[11px] font-bold uppercase tracking-wider text-zinc-500">
                    <span class="rounded-full {{ $isStillLiveInDesk ? 'bg-rose-100 text-rose-800 dark:bg-rose-950 dark:text-rose-300' : 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300' }} px-2.5 py-1">
                        {{ $isStillLiveInDesk ? 'Favourite' : 'Saved snapshot' }}
                    </span>
                    <span>{{ \App\Support\SourceCatalog::label($favourite->source) }}</span>
                    <span>Code {{ $codeGrade }}</span>
                    <x-text-scan-badge :checked="$hasTextScan" :grade="$textScanGrade" :review="$textScanNeedsReview" compact />
                    <x-visual-badge :checked="$hasVisualCheck" :grade="$visualGrade" :review="$visualNeedsReview" compact />
                    <x-effective-grade-badge :grade="$effectiveGrade" compact />
                </div>
                <a href="{{ route('favourites.show', $favourite) }}" wire:navigate class="text-lg font-semibold leading-snug text-zinc-950 group-hover:text-emerald-700 dark:text-white dark:group-hover:text-emerald-400">
                    {{ $favourite->title }}
                </a>
                <p class="mt-1 truncate font-mono text-[11px] text-zinc-500">{{ $favourite->model_key }}</p>
            </div>
            <div class="shrink-0 sm:text-right">
                <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Expected profit</p>
                <p class="text-3xl font-semibold tabular-nums text-emerald-700 dark:text-emerald-400">£{{ number_format((float) $favourite->expected_profit, 2) }}</p>
                <p class="text-xs text-zinc-500">{{ number_format((float) $favourite->expected_roi * 100) }}% ROI</p>
            </div>
        </div>

        <dl class="mt-5 grid grid-cols-2 divide-x divide-y divide-zinc-200 border-y border-zinc-200 text-sm sm:grid-cols-4 sm:divide-y-0 dark:divide-zinc-700 dark:border-zinc-700">
            <div class="py-3 pr-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Current</dt><dd class="mt-0.5 text-lg font-medium tabular-nums">£{{ number_format((float) $favourite->current_price, 2) }}</dd></div>
            <div class="p-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Max safe buy</dt><dd class="mt-0.5 text-lg font-medium tabular-nums">£{{ number_format((float) $favourite->max_safe_buy, 2) }}</dd></div>
            <div class="p-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Sold median</dt><dd class="mt-0.5 text-lg font-medium tabular-nums">{{ $hasEvidence ? '£'.number_format((float) $favourite->sold_median, 2) : '—' }}</dd></div>
            <div class="py-3 pl-3"><dt class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Evidence</dt><dd class="mt-0.5 text-lg font-medium tabular-nums">{{ $hasEvidence ? number_format($favourite->comp_samples).' sold' : 'No comps' }}</dd></div>
        </dl>

        <div class="mt-4 flex flex-col justify-between gap-3 text-xs text-zinc-500 sm:flex-row sm:items-center">
            <p>
                {{ $favourite->end_time?->isFuture() ? 'Ends '.$favourite->end_time->diffForHumans() : 'End time passed' }}
                · saved {{ $favourite->favourited_at->diffForHumans() }}
            </p>
            <div class="flex flex-wrap items-center gap-3">
                <a href="{{ $favourite->url }}" target="_blank" rel="noreferrer" class="font-medium underline decoration-zinc-300 underline-offset-4">View on eBay ↗</a>
                <form method="POST" action="{{ route('favourites.destroy', $favourite) }}">
                    @csrf
                    @method('DELETE')
                    <flux:button type="submit" size="sm" variant="ghost">Remove favourite</flux:button>
                </form>
                <flux:button :href="route('favourites.show', $favourite)" wire:navigate size="sm" variant="primary">{{ $hasEvidence ? 'View saved evidence' : 'View snapshot' }}</flux:button>
            </div>
        </div>
    </div>
</article>
