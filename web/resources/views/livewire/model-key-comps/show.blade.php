<div class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7">
    <div><flux:button :href="route('search.index', ['search' => $modelKeyComp->model_key])" wire:navigate variant="ghost" icon="arrow-left">Back to search</flux:button></div>

    <section class="rounded-3xl bg-[#10251b] p-7 text-white shadow-xl sm:p-10">
        <div class="flex flex-col justify-between gap-8 lg:flex-row lg:items-end">
            <div class="max-w-5xl">
                <div class="mb-4 flex flex-wrap gap-2 text-[10px] font-bold uppercase tracking-widest text-emerald-100/70">
                    <span class="rounded-full bg-lime-300 px-2.5 py-1 text-emerald-950">Market evidence</span>
                    <span class="py-1">{{ \App\Support\SourceCatalog::label($modelKeyComp->source) }}</span>
                    <x-effective-grade-badge :grade="$modelKeyComp->condition_grade" compact />
                </div>
                <h1 class="break-all text-3xl font-semibold leading-tight tracking-tight sm:text-5xl">{{ $modelKeyComp->model_key }}</h1>
                <p class="mt-4 text-sm text-emerald-100/70">{{ number_format($modelKeyComp->samples) }} exact sold comps · median £{{ number_format((float) $modelKeyComp->median_final_price, 2) }}</p>
            </div>
            <div class="flex flex-wrap gap-2">
                @foreach($gradeSummaries as $summary)
                    <flux:button :href="route('model-key-comps.show', $summary)" wire:navigate :variant="$summary->id === $modelKeyComp->id ? 'primary' : 'ghost'">
                        Grade {{ $summary->condition_grade }}
                    </flux:button>
                @endforeach
            </div>
        </div>
    </section>

    <section class="grid grid-cols-2 gap-3 lg:grid-cols-4">
        <div class="rounded-2xl bg-emerald-700 p-5 text-white"><p class="text-[10px] font-bold uppercase tracking-widest text-emerald-100">Sold median</p><p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format((float) $modelKeyComp->median_final_price, 2) }}</p><p class="text-xs text-emerald-100">{{ number_format($modelKeyComp->samples) }} exact comps</p></div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900"><p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Suggested BIN</p><p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format($math['suggestedBin'], 0) }}</p><p class="text-xs text-zinc-500">95% of median</p></div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900"><p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Offer floor</p><p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format($math['offerFloor'], 0) }}</p><p class="text-xs text-zinc-500">operator guideline</p></div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900"><p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Max safe buy</p><p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format($math['maxSafeBuy'], 2) }}</p><p class="text-xs text-zinc-500">at £50 profit</p></div>
    </section>

    <section class="grid items-start gap-8 lg:grid-cols-[minmax(0,1fr)_340px]">
        <div>
            <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end"><div><p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-emerald-400">Confirmed outcomes</p><h2 class="text-4xl font-semibold tracking-tight">Sold evidence</h2></div><p class="max-w-sm text-sm text-zinc-500">Same model key and condition grade, nearest to the median first.</p></div>
            <div class="grid gap-2">
                @forelse($rows as $row)
                    <article class="grid grid-cols-[1fr_auto] gap-4 rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
                        <div>
                            <div class="mb-1 flex flex-wrap items-center gap-2">
                                <x-text-scan-badge :checked="$row->text_scan_checked" :grade="$row->text_scan_grade" :review="$row->text_scan_review_required" compact />
                                <x-visual-badge :checked="$row->visual_condition_checked" :grade="$row->visual_condition_grade" :review="$row->visual_review_required" compact />
                                <x-effective-grade-badge :grade="$modelKeyComp->condition_grade" compact />
                            </div>
                            <p class="text-lg font-semibold tabular-nums text-emerald-700 dark:text-emerald-400">£{{ number_format((float) $row->final_price, 2) }}</p>
                            <h3 class="mt-0.5 text-sm font-semibold leading-snug">{{ $row->title }}</h3>
                            <p class="mt-1 text-[11px] text-zinc-500">{{ str($row->sale_type)->replace('_', ' ') }} · {{ $row->bids_count ?? 0 }} bids {{ $row->end_time ? '· sold '.$row->end_time->format('j M Y') : '' }}</p>
                        </div>
                        @if($row->url)<a href="{{ $row->url }}" target="_blank" rel="noreferrer" class="self-center font-bold text-emerald-700">↗</a>@endif
                    </article>
                @empty
                    <div class="rounded-2xl border border-dashed border-zinc-300 p-8 text-center text-sm text-zinc-500 dark:border-zinc-700">
                        No sold rows have been pushed for this model key yet.
                    </div>
                @endforelse
            </div>
        </div>

        <aside class="rounded-2xl bg-zinc-950 p-6 text-white shadow-[7px_7px_0_#bef264] lg:sticky lg:top-5">
            <p class="text-xs font-bold uppercase tracking-[0.18em] text-lime-300">The maths</p><h2 class="mt-1 text-3xl font-semibold tracking-tight">From resale to net</h2>
            <dl class="mt-5 divide-y divide-zinc-700 text-sm">
                <div class="flex justify-between py-3"><dt>Sold median</dt><dd class="font-bold">£{{ number_format((float) $modelKeyComp->median_final_price, 2) }}</dd></div>
                <div class="flex justify-between py-3 text-zinc-400"><dt>eBay fees · 13%</dt><dd>- £{{ number_format($math['fees'], 2) }}</dd></div>
                <div class="flex justify-between py-3 text-zinc-400"><dt>Outbound postage</dt><dd>- £{{ number_format($math['outboundPostage'], 2) }}</dd></div>
                <div class="flex justify-between py-3 text-zinc-400"><dt>Packaging</dt><dd>- £{{ number_format($math['packaging'], 2) }}</dd></div>
                <div class="flex justify-between py-3 text-zinc-400"><dt>Selling reserve</dt><dd>- £{{ number_format($math['sellingReserve'], 2) }}</dd></div>
                <div class="flex justify-between py-4 text-lg font-semibold tabular-nums text-lime-300"><dt>Net if already owned</dt><dd>£{{ number_format($math['netOwned'], 2) }}</dd></div>
            </dl>
            <p class="mt-3 border-t border-zinc-700 pt-4 text-[11px] leading-5 text-zinc-400">Max safe buy also subtracts £{{ number_format($math['inboundPostage'], 2) }} inbound postage and a £50 profit gate.</p>
        </aside>
    </section>
</div>
