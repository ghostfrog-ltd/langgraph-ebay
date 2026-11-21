@php
    $fees = (float) $favourite->sold_median * 0.13;
    $hasEvidence = (int) $favourite->comp_samples > 0 && (float) $favourite->sold_median > 0;
    $currentListing = $favourite->currentListing;
    $effectiveGrade = $favourite->condition_grade ?: $currentListing?->condition_grade;
    $codeGrade = $favourite->text_condition_grade ?: $currentListing?->text_condition_grade ?: $effectiveGrade;
    $visualGrade = $favourite->visual_condition_grade ?: $currentListing?->visual_condition_grade;
    $hasVisualCheck = (bool) ($favourite->visual_condition_checked || $currentListing?->visual_condition_checked);
    $visualNeedsReview = (bool) ($favourite->visual_review_required || $currentListing?->visual_review_required);
    $textScanGrade = $favourite->text_scan_grade ?: $currentListing?->text_scan_grade;
    $hasTextScan = (bool) ($favourite->text_scan_checked || $currentListing?->text_scan_checked);
    $textScanNeedsReview = (bool) ($favourite->text_scan_review_required || $currentListing?->text_scan_review_required);
@endphp

<div class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7">
    <div class="flex flex-wrap items-center gap-3">
        <flux:button :href="route('favourites.index')" wire:navigate variant="ghost" icon="arrow-left">Back to favourites</flux:button>
        @if($currentListing)
            <flux:button :href="route('listings.show', $currentListing)" wire:navigate variant="ghost">Open live desk row</flux:button>
        @endif
        <form method="POST" action="{{ route('favourites.destroy', $favourite) }}">
            @csrf
            @method('DELETE')
            <flux:button type="submit" variant="ghost">Remove favourite</flux:button>
        </form>
    </div>

    <section class="rounded-3xl bg-[#10251b] p-7 text-white shadow-xl sm:p-10">
        <div class="flex flex-col justify-between gap-8 lg:flex-row lg:items-end">
            <div class="max-w-5xl">
                <div class="mb-4 flex flex-wrap gap-2 text-[10px] font-bold uppercase tracking-widest text-emerald-100/70">
                    <span class="rounded-full bg-rose-300 px-2.5 py-1 text-rose-950">Favourite</span>
                    <span class="py-1">{{ \App\Support\SourceCatalog::label($favourite->source) }}</span>
                    <span class="py-1">Code {{ $codeGrade }}</span>
                    <x-text-scan-badge :checked="$hasTextScan" :grade="$textScanGrade" :review="$textScanNeedsReview" compact />
                    <x-visual-badge :checked="$hasVisualCheck" :grade="$visualGrade" :review="$visualNeedsReview" compact />
                    <x-effective-grade-badge :grade="$effectiveGrade" compact />
                    <span class="py-1">Item {{ $favourite->external_id }}</span>
                    @unless($currentListing)
                        <span class="py-1">Saved snapshot</span>
                    @endunless
                </div>
                <h1 class="text-4xl font-semibold leading-tight tracking-tight sm:text-5xl">{{ $favourite->title }}</h1>
                <p class="mt-4 break-all font-mono text-xs text-emerald-100/60">
                    {{ $favourite->model_key }} · Code {{ $codeGrade }} · Effective {{ $effectiveGrade }}
                    @if($hasTextScan && $textScanGrade)
                        · Text scan {{ $textScanGrade }}
                    @endif
                    @if($hasVisualCheck && $visualGrade)
                        · Visual {{ $visualGrade }}
                    @endif
                </p>
            </div>
            <flux:button href="{{ $favourite->url }}" target="_blank" variant="filled">Open on eBay ↗</flux:button>
        </div>
    </section>

    <section class="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <div class="rounded-2xl {{ $hasEvidence ? 'bg-emerald-700 text-white' : 'border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900' }} p-5"><p class="text-[10px] font-bold uppercase tracking-widest {{ $hasEvidence ? 'text-emerald-100' : 'text-zinc-500' }}">{{ $hasEvidence ? 'Expected profit' : 'Evidence status' }}</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ $hasEvidence ? '£'.number_format((float) $favourite->expected_profit, 2) : 'No comps' }}</p><p class="text-xs {{ $hasEvidence ? 'text-emerald-100' : 'text-zinc-500' }}">{{ $hasEvidence ? number_format((float) $favourite->expected_roi * 100).'% ROI' : 'browse-only listing' }}</p></div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900"><p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Saved price</p><p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format((float) $favourite->current_price, 2) }}</p><p class="text-xs text-zinc-500">at favourite time</p></div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900"><p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Max safe buy</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ $hasEvidence ? '£'.number_format((float) $favourite->max_safe_buy, 2) : '—' }}</p><p class="text-xs text-zinc-500">saved maths</p></div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900"><p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Sold median</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ $hasEvidence ? '£'.number_format((float) $favourite->sold_median, 2) : '—' }}</p><p class="text-xs text-zinc-500">{{ $hasEvidence ? number_format($favourite->comp_samples).' saved comps' : 'no same-key evidence saved' }}</p></div>
    </section>

    <section class="grid items-start gap-8 lg:grid-cols-[minmax(0,1fr)_340px]">
        <div>
            <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end"><div><p class="text-xs font-bold uppercase tracking-[0.18em] text-rose-700 dark:text-rose-400">Saved outcomes</p><h2 class="text-4xl font-semibold tracking-tight">Favourite evidence</h2></div><p class="max-w-sm text-sm text-zinc-500">Comp snapshot captured when this listing was favourited.</p></div>
            <div class="grid gap-2">
                @forelse($comps as $comp)
                    @php
                        $soldAt = isset($comp['end_time']) && $comp['end_time'] ? \Illuminate\Support\Carbon::parse($comp['end_time']) : null;
                    @endphp
                    <article class="grid grid-cols-[1fr_auto] gap-4 rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
                        <div>
                            <div class="mb-1 flex flex-wrap items-center gap-2">
                                <x-text-scan-badge :checked="(bool) ($comp['text_scan_checked'] ?? false)" :grade="$comp['text_scan_grade'] ?? null" :review="(bool) ($comp['text_scan_review_required'] ?? false)" compact />
                                <x-visual-badge :checked="(bool) ($comp['visual_condition_checked'] ?? false)" :grade="$comp['visual_condition_grade'] ?? null" :review="(bool) ($comp['visual_review_required'] ?? false)" compact />
                                <x-effective-grade-badge :grade="$comp['condition_grade'] ?? null" compact />
                            </div>
                            <p class="text-lg font-semibold tabular-nums text-emerald-700 dark:text-emerald-400">£{{ number_format((float) ($comp['final_price'] ?? 0), 2) }}</p>
                            <h3 class="mt-0.5 text-sm font-semibold leading-snug">{{ $comp['title'] ?? 'Saved comp' }}</h3>
                            <p class="mt-1 text-[11px] text-zinc-500">{{ str((string) ($comp['sale_type'] ?? ''))->replace('_', ' ') }} · {{ $comp['bids_count'] ?? 0 }} bids {{ $soldAt ? '· sold '.$soldAt->format('j M Y') : '' }}</p>
                        </div>
                        @if(! empty($comp['url']))<a href="{{ $comp['url'] }}" target="_blank" rel="noreferrer" class="self-center font-bold text-emerald-700">↗</a>@endif
                    </article>
                @empty
                    <div class="rounded-2xl border border-dashed border-zinc-300 p-8 text-center text-sm text-zinc-500 dark:border-zinc-700">
                        No sold evidence snapshot was attached when this listing was favourited.
                    </div>
                @endforelse
            </div>
        </div>

        @if($hasEvidence)
            <aside class="rounded-2xl bg-zinc-950 p-6 text-white shadow-[7px_7px_0_#fda4af] lg:sticky lg:top-5">
                <p class="text-xs font-bold uppercase tracking-[0.18em] text-rose-300">Saved maths</p><h2 class="mt-1 text-3xl font-semibold tracking-tight">Why it was saved</h2>
                <dl class="mt-5 divide-y divide-zinc-700 text-sm">
                    <div class="flex justify-between py-3"><dt>Sold median</dt><dd class="font-bold">£{{ number_format((float) $favourite->sold_median, 2) }}</dd></div>
                    <div class="flex justify-between py-3 text-zinc-400"><dt>eBay fees · 13%</dt><dd>− £{{ number_format($fees, 2) }}</dd></div>
                    <div class="flex justify-between py-3 text-zinc-400"><dt>Saved buy price</dt><dd>− £{{ number_format((float) $favourite->current_price, 2) }}</dd></div>
                    <div class="flex justify-between py-4 text-lg font-semibold tabular-nums text-rose-300"><dt>Expected profit</dt><dd>£{{ number_format((float) $favourite->expected_profit, 2) }}</dd></div>
                </dl>
                <p class="mt-3 border-t border-zinc-700 pt-4 text-[11px] leading-5 text-zinc-400">This is the saved evidence snapshot. It may differ from the current live-listing table after later syncs.</p>
            </aside>
        @else
            <aside class="rounded-2xl border border-zinc-200 bg-white p-6 dark:border-zinc-700 dark:bg-zinc-900 lg:sticky lg:top-5">
                <p class="text-xs font-bold uppercase tracking-[0.18em] text-zinc-500">Saved snapshot</p><h2 class="mt-1 text-3xl font-semibold tracking-tight">No resale maths saved</h2>
                <p class="mt-4 text-sm leading-6 text-zinc-500">{{ $favourite->signal }}</p>
            </aside>
        @endif
    </section>
</div>
