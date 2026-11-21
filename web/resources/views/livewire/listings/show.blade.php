@php
    $fees = (float) $listing->sold_median * 0.13;
    $hasEvidence = (int) $listing->comp_samples > 0 && (float) $listing->sold_median > 0;
    $fixedCosts = 17.0;
    $codeGrade = $listing->text_condition_grade ?: $listing->condition_grade;
    $effectiveGrade = $listing->condition_grade;
    $textScanGrade = $listing->text_scan_grade;
    $hasTextScan = (bool) $listing->text_scan_checked;
    $textScanNeedsReview = (bool) $listing->text_scan_review_required;
    $textScanRiskFlags = collect($listing->text_scan_risk_flags ?? [])
        ->map(fn ($flag) => str($flag)->replace('_', ' ')->title())
        ->all();
    $visualGrade = $listing->visual_condition_grade;
    $hasVisualCheck = (bool) $listing->visual_condition_checked;
    $visualNeedsReview = (bool) $listing->visual_review_required;
    $visualDamageFlags = collect($listing->visual_damage_flags ?? [])
        ->map(fn ($flag) => str($flag)->replace('_', ' ')->title())
        ->all();
@endphp

<div
    class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7"
    x-data="{
        currentPrice: {{ (float) $listing->current_price }},
        originalPrice: {{ (float) $listing->current_price }},
        soldMedian: {{ (float) $listing->sold_median }},
        fees: {{ $fees }},
        fixedCosts: {{ $fixedCosts }},
        maxSafeBuy: {{ (float) $listing->max_safe_buy }},
        hasEvidence: {{ $hasEvidence ? 'true' : 'false' }},
        money(value) {
            const amount = Number(value) || 0;
            const sign = amount < 0 ? '-' : '';
            return `${sign}£${Math.abs(amount).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        },
        deduction(value) {
            return `− ${this.money(value)}`;
        },
        profit() {
            if (! this.hasEvidence) {
                return 0;
            }
            return this.soldMedian - this.fees - this.fixedCosts - (Number(this.currentPrice) || 0);
        },
        roi() {
            const price = Number(this.currentPrice) || 0;
            if (! this.hasEvidence || price <= 0) {
                return '— ROI';
            }
            return `${Math.round((this.profit() / price) * 100).toLocaleString('en-GB')}% ROI`;
        },
        gateNote() {
            if (! this.hasEvidence) {
                return 'live listing price';
            }
            const gap = this.maxSafeBuy - (Number(this.currentPrice) || 0);
            return gap >= 0 ? `${this.money(gap)} under max safe buy` : `${this.money(Math.abs(gap))} over max safe buy`;
        },
        resetPrice() {
            this.currentPrice = this.originalPrice;
        },
    }"
>
    <div><flux:button :href="route('dashboard')" wire:navigate variant="ghost" icon="arrow-left">Back to live listings</flux:button></div>

    <section class="rounded-3xl bg-[#10251b] p-7 text-white shadow-xl sm:p-10">
        <div class="flex flex-col justify-between gap-8 lg:flex-row lg:items-end">
            <div class="max-w-5xl">
                <div class="mb-4 flex flex-wrap gap-2 text-[10px] font-bold uppercase tracking-widest text-emerald-100/70">
                    <span class="rounded-full bg-emerald-300 px-2.5 py-1 text-emerald-950">{{ $hasEvidence ? 'Evidence ready' : 'Browse row' }}</span>
                    <span class="py-1">{{ \App\Support\SourceCatalog::label($listing->source) }}</span>
                    <span class="py-1">Code {{ $codeGrade }}</span>
                    <x-text-scan-badge :checked="$hasTextScan" :grade="$textScanGrade" :review="$textScanNeedsReview" compact />
                    <x-visual-badge :checked="$hasVisualCheck" :grade="$visualGrade" :review="$visualNeedsReview" compact />
                    <x-effective-grade-badge :grade="$effectiveGrade" compact />
                    <span class="py-1">Item {{ $listing->external_id }}</span>
                </div>
                <h1 class="text-4xl font-semibold leading-tight tracking-tight sm:text-5xl">{{ $listing->title }}</h1>
                <p class="mt-4 break-all font-mono text-xs text-emerald-100/60">
                    {{ $listing->model_key }} · Code {{ $codeGrade }} · Effective {{ $effectiveGrade }}
                    @if($hasTextScan && $textScanGrade)
                        · Text scan {{ $textScanGrade }}
                    @endif
                    @if($hasVisualCheck && $visualGrade)
                        · Visual {{ $visualGrade }}
                    @endif
                </p>
                @if($hasTextScan && $textScanRiskFlags)
                    <p class="mt-2 text-xs text-emerald-100/70">Text scan flags: {{ implode(', ', $textScanRiskFlags) }}</p>
                @endif
                @if($hasVisualCheck && $visualDamageFlags)
                    <p class="mt-2 text-xs text-emerald-100/70">Visual flags: {{ implode(', ', $visualDamageFlags) }}</p>
                @endif
            </div>
            <div class="flex flex-wrap gap-3">
                @if($favourite)
                    <form method="POST" action="{{ route('favourites.destroy', $favourite) }}">
                        @csrf
                        @method('DELETE')
                        <button type="submit" class="inline-flex items-center justify-center rounded-xl bg-white px-4 py-2.5 text-sm font-bold text-emerald-950 shadow-sm transition hover:bg-emerald-50 focus:outline-none focus:ring-2 focus:ring-lime-300">
                            Remove favourite
                        </button>
                    </form>
                @else
                    <form method="POST" action="{{ route('listings.favourites.store', $listing) }}">
                        @csrf
                        <button type="submit" class="inline-flex items-center justify-center rounded-xl bg-white px-4 py-2.5 text-sm font-bold text-emerald-950 shadow-sm transition hover:bg-emerald-50 focus:outline-none focus:ring-2 focus:ring-lime-300">
                            Add favourite
                        </button>
                    </form>
                @endif
                <a href="{{ $listing->url }}" target="_blank" rel="noreferrer" class="inline-flex items-center justify-center rounded-xl bg-lime-300 px-4 py-2.5 text-sm font-bold text-zinc-950 shadow-sm transition hover:bg-lime-200 focus:outline-none focus:ring-2 focus:ring-white">
                    Open live listing ↗
                </a>
            </div>
        </div>
    </section>

    <section class="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <div class="rounded-2xl {{ $hasEvidence ? 'bg-emerald-700 text-white' : 'border border-zinc-200 bg-white dark:border-zinc-700 dark:bg-zinc-900' }} p-5">
            <p class="text-[10px] font-bold uppercase tracking-widest {{ $hasEvidence ? 'text-emerald-100' : 'text-zinc-500' }}">{{ $hasEvidence ? 'Expected profit' : 'Evidence status' }}</p>
            <p class="mt-1 text-3xl font-semibold tabular-nums" @if($hasEvidence) x-text="money(profit())" @endif>{{ $hasEvidence ? '£'.number_format((float) $listing->expected_profit, 2) : 'No comps' }}</p>
            <p class="text-xs {{ $hasEvidence ? 'text-emerald-100' : 'text-zinc-500' }}" @if($hasEvidence) x-text="roi()" @endif>{{ $hasEvidence ? number_format((float) $listing->expected_roi * 100).'% ROI' : 'browse-only listing' }}</p>
        </div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900">
            <div class="flex items-center justify-between gap-3">
                <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Current price</p>
                <button
                    type="button"
                    class="text-[10px] font-bold uppercase tracking-widest text-emerald-700 underline-offset-2 hover:underline dark:text-emerald-400"
                    x-show="Number(currentPrice) !== Number(originalPrice)"
                    x-cloak
                    @click="resetPrice()"
                >
                    reset
                </button>
            </div>
            <label class="mt-2 flex items-center gap-2 rounded-xl border border-zinc-200 bg-zinc-50 px-3 py-2 focus-within:border-emerald-600 focus-within:ring-2 focus-within:ring-emerald-100 dark:border-zinc-700 dark:bg-zinc-950 dark:focus-within:border-emerald-400 dark:focus-within:ring-emerald-900/40">
                <span class="text-2xl font-semibold tabular-nums text-zinc-500">£</span>
                <input
                    type="number"
                    min="0"
                    step="1"
                    inputmode="decimal"
                    x-model.number="currentPrice"
                    class="w-full border-0 bg-transparent p-0 text-3xl font-semibold tabular-nums text-zinc-950 shadow-none outline-none focus:ring-0 dark:text-white"
                    aria-label="Current price"
                />
            </label>
            <p class="mt-1 text-xs text-zinc-500" x-text="gateNote()">live listing price</p>
        </div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900"><p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Max safe buy</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ $hasEvidence ? '£'.number_format((float) $listing->max_safe_buy, 2) : '—' }}</p><p class="text-xs text-zinc-500">at £50 profit</p></div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900"><p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Sold median</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ $hasEvidence ? '£'.number_format((float) $listing->sold_median, 2) : '—' }}</p><p class="text-xs text-zinc-500">{{ $hasEvidence ? number_format($listing->comp_samples).' exact sold comps' : 'no same-key evidence yet' }}</p></div>
    </section>

    <section class="grid items-start gap-8 lg:grid-cols-[minmax(0,1fr)_340px]">
        <div>
            <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end"><div><p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-emerald-400">Confirmed outcomes</p><h2 class="text-4xl font-semibold tracking-tight">Sold evidence</h2></div><p class="max-w-sm text-sm text-zinc-500">Same model key and condition grade, nearest to the median first.</p></div>
            <div class="grid gap-2">
                @forelse($comps as $comp)
                    <article class="grid grid-cols-[1fr_auto] gap-4 rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
                        <div>
                            <div class="mb-1 flex flex-wrap items-center gap-2">
                                <x-text-scan-badge :checked="$comp->text_scan_checked" :grade="$comp->text_scan_grade" :review="$comp->text_scan_review_required" compact />
                                <x-visual-badge :checked="$comp->visual_condition_checked" :grade="$comp->visual_condition_grade" :review="$comp->visual_review_required" compact />
                                <x-effective-grade-badge :grade="$comp->condition_grade" compact />
                            </div>
                            <p class="text-lg font-semibold tabular-nums text-emerald-700 dark:text-emerald-400">£{{ number_format((float) $comp->final_price, 2) }}</p>
                            <h3 class="mt-0.5 text-sm font-semibold leading-snug">{{ $comp->title }}</h3>
                            <p class="mt-1 text-[11px] text-zinc-500">{{ str($comp->sale_type)->replace('_', ' ') }} · {{ $comp->bids_count ?? 0 }} bids {{ $comp->end_time ? '· sold '.$comp->end_time->format('j M Y') : '' }}</p>
                        </div>
                        @if($comp->url)<a href="{{ $comp->url }}" target="_blank" rel="noreferrer" class="self-center font-bold text-emerald-700">↗</a>@endif
                    </article>
                @empty
                    <div class="rounded-2xl border border-dashed border-zinc-300 p-8 text-center text-sm text-zinc-500 dark:border-zinc-700">
                        No same-key, same-grade sold evidence is attached to this listing yet.
                    </div>
                @endforelse
            </div>
        </div>

        @if($hasEvidence)
            <aside class="rounded-2xl bg-zinc-950 p-6 text-white shadow-[7px_7px_0_#bef264] lg:sticky lg:top-5">
                <p class="text-xs font-bold uppercase tracking-[0.18em] text-lime-300">The maths</p><h2 class="mt-1 text-3xl font-semibold tracking-tight">From resale to profit</h2>
                <dl class="mt-5 divide-y divide-zinc-700 text-sm">
                    <div class="flex justify-between py-3"><dt>Sold median</dt><dd class="font-bold">£{{ number_format((float) $listing->sold_median, 2) }}</dd></div>
                    <div class="flex justify-between py-3 text-zinc-400"><dt>eBay fees · 13%</dt><dd>− £{{ number_format($fees, 2) }}</dd></div>
                    <div class="flex justify-between py-3 text-zinc-400"><dt>Outbound postage</dt><dd>− £7.00</dd></div>
                    <div class="flex justify-between py-3 text-zinc-400"><dt>Inbound postage</dt><dd>− £5.00</dd></div>
                    <div class="flex justify-between py-3 text-zinc-400"><dt>Packaging</dt><dd>− £2.00</dd></div>
                    <div class="flex justify-between py-3 text-zinc-400"><dt>Selling reserve</dt><dd>− £3.00</dd></div>
                    <div class="flex justify-between py-3 text-zinc-400"><dt>Current buy price</dt><dd x-text="deduction(currentPrice)">− £{{ number_format((float) $listing->current_price, 2) }}</dd></div>
                    <div class="flex justify-between py-4 text-lg font-semibold tabular-nums text-lime-300"><dt>Expected profit</dt><dd x-text="money(profit())">£{{ number_format((float) $listing->expected_profit, 2) }}</dd></div>
                </dl>
                <p class="mt-3 border-t border-zinc-700 pt-4 text-[11px] leading-5 text-zinc-400">Evidence supports the estimate; it does not verify this item's condition, completeness, seller, or authenticity.</p>
            </aside>
        @else
            <aside class="rounded-2xl border border-zinc-200 bg-white p-6 dark:border-zinc-700 dark:bg-zinc-900 lg:sticky lg:top-5">
                <p class="text-xs font-bold uppercase tracking-[0.18em] text-zinc-500">Browse-only</p><h2 class="mt-1 text-3xl font-semibold tracking-tight">No resale maths yet</h2>
                <p class="mt-4 text-sm leading-6 text-zinc-500">{{ $listing->signal }}</p>
                <p class="mt-4 text-xs leading-5 text-zinc-500">This row is still useful for source browsing, but it is not a buy recommendation until clean same-key sold evidence exists.</p>
            </aside>
        @endif
    </section>
</div>
