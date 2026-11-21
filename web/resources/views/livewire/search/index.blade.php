<div class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7">
    <section class="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="flex flex-col justify-between gap-3 lg:flex-row lg:items-end">
            <div>
                <p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-emerald-400">Evidence search</p>
                <h1 class="text-3xl font-semibold tracking-tight">Find anything we know</h1>
            </div>
            <p class="max-w-xl text-sm text-zinc-500">
                Broad search across live listings, sold evidence, and your saved favourites. This page ignores the buy-desk hiding rules and labels the reason instead.
            </p>
        </div>
    </section>

    <section class="rounded-2xl border border-zinc-200 bg-white p-3 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="flex flex-col gap-3 sm:flex-row sm:items-end">
            <flux:input wire:model.live.debounce.300ms="search" label="Search" placeholder="Try macbook pro m1 16gb, RTX 4090, item ID" autofocus class="flex-1" />
            @if($search !== '')
                <flux:button wire:click="clearSearch" variant="ghost">Clear</flux:button>
            @endif
        </div>
    </section>

    @if(! $hasSearch)
        <section class="rounded-2xl border border-dashed border-zinc-300 p-10 text-center dark:border-zinc-700">
            <p class="text-2xl font-semibold">Search for a model, spec, source, or eBay item ID.</p>
            <p class="mt-2 text-sm text-zinc-500">This is the rummage drawer: useful when the tidy buy desk has hidden something you want to investigate.</p>
        </section>
    @else
        <section class="grid grid-cols-2 overflow-hidden rounded-2xl border border-zinc-200 bg-white md:grid-cols-4 dark:border-zinc-700 dark:bg-zinc-900">
            <div class="border-r border-zinc-200 p-5 dark:border-zinc-700"><p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Live listings</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($listingCount) }}</p><p class="text-xs text-zinc-500">before buy filters</p></div>
            <div class="border-r border-zinc-200 p-5 dark:border-zinc-700"><p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Sold evidence</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($compCount) }}</p><p class="text-xs text-zinc-500">all mirrored sold rows</p></div>
            <div class="border-r border-zinc-200 p-5 dark:border-zinc-700"><p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Market evidence</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($modelKeyCompCount) }}</p><p class="text-xs text-zinc-500">model-key summaries</p></div>
            <div class="p-5"><p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Favourites</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($favouriteCount) }}</p><p class="text-xs text-zinc-500">your saved rows</p></div>
        </section>

        <section>
            <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
                <div>
                    <p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-emerald-400">Live rows</p>
                    <h2 class="text-4xl font-semibold tracking-tight">Listings</h2>
                </div>
                <p class="max-w-lg text-sm text-zinc-500">These can be profitable, too expensive, thin evidence, no-comps, or hidden-source rows.</p>
            </div>
            <div class="grid gap-4">
                @forelse($listings as $listing)
                    <x-listing-card :listing="$listing" mode="search" />
                @empty
                    <div class="rounded-2xl border border-dashed border-zinc-300 p-7 text-center text-sm text-zinc-500 dark:border-zinc-700">No live listings matched this search.</div>
                @endforelse
            </div>
            @if($listingCount > $listings->count())
                <p class="mt-3 text-xs text-zinc-500">Showing the first {{ number_format($listings->count()) }} live listing matches.</p>
            @endif
        </section>

        <section>
            <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
                <div>
                    <p class="text-xs font-bold uppercase tracking-[0.18em] text-lime-700 dark:text-lime-400">Model keys</p>
                    <h2 class="text-4xl font-semibold tracking-tight">Market evidence</h2>
                </div>
                <p class="max-w-lg text-sm text-zinc-500">Published comp summaries by strict model key and condition grade.</p>
            </div>
            <div class="grid gap-3">
                @forelse($modelKeyComps as $modelKeyComp)
                    <article class="rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
                        <div class="flex flex-col justify-between gap-4 sm:flex-row sm:items-start">
                            <div class="min-w-0">
                                <div class="mb-2 flex flex-wrap items-center gap-2 text-[11px] font-bold uppercase tracking-wider text-zinc-500">
                                    <span class="rounded-full bg-lime-100 px-2.5 py-1 text-lime-800 dark:bg-lime-950 dark:text-lime-300">Market evidence</span>
                                    <span>{{ \App\Support\SourceCatalog::label($modelKeyComp->source) }}</span>
                                    <x-effective-grade-badge :grade="$modelKeyComp->condition_grade" compact />
                                </div>
                                <p class="break-all font-mono text-sm text-zinc-500">{{ $modelKeyComp->model_key }}</p>
                            </div>
                            <div class="grid grid-cols-2 gap-4 text-right sm:min-w-80">
                                <div>
                                    <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Sold median</p>
                                    <p class="mt-1 text-3xl font-semibold tabular-nums text-emerald-700 dark:text-emerald-400">£{{ number_format((float) $modelKeyComp->median_final_price, 2) }}</p>
                                </div>
                                <div>
                                    <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Comps</p>
                                    <p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($modelKeyComp->samples) }}</p>
                                </div>
                            </div>
                        </div>
                        <div class="mt-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                            <p class="text-[11px] text-zinc-500">
                                {{ $modelKeyComp->computed_at ? 'Computed '.$modelKeyComp->computed_at->format('j M Y H:i') : 'Computed time unknown' }}
                                · synced {{ $modelKeyComp->synced_at->diffForHumans() }}
                            </p>
                            <flux:button :href="route('model-key-comps.show', $modelKeyComp)" wire:navigate size="sm" variant="primary">Inspect comps</flux:button>
                        </div>
                    </article>
                @empty
                    <div class="rounded-2xl border border-dashed border-zinc-300 p-7 text-center text-sm text-zinc-500 dark:border-zinc-700">No model-key market evidence matched this search.</div>
                @endforelse
            </div>
            @if($modelKeyCompCount > $modelKeyComps->count())
                <p class="mt-3 text-xs text-zinc-500">Showing the first {{ number_format($modelKeyComps->count()) }} market evidence matches.</p>
            @endif
        </section>

        <section>
            <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
                <div>
                    <p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-emerald-400">Confirmed outcomes</p>
                    <h2 class="text-4xl font-semibold tracking-tight">Sold evidence</h2>
                </div>
                <p class="max-w-lg text-sm text-zinc-500">All confirmed historical sold rows mirrored from the local evidence database.</p>
            </div>
            <div class="grid gap-2">
                @forelse($comps as $comp)
                    <article class="grid gap-4 rounded-xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900 sm:grid-cols-[1fr_auto] sm:items-center">
                        <div class="min-w-0">
                            <div class="mb-1 flex flex-wrap items-center gap-2 text-[11px] font-bold uppercase tracking-wider text-zinc-500">
                                <span class="rounded-full bg-emerald-100 px-2.5 py-1 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-300">Sold evidence</span>
                                @if($comp->source)<span>{{ \App\Support\SourceCatalog::label($comp->source) }}</span>@endif
                                <x-text-scan-badge :checked="$comp->text_scan_checked" :grade="$comp->text_scan_grade" :review="$comp->text_scan_review_required" compact />
                                <x-visual-badge :checked="$comp->visual_condition_checked" :grade="$comp->visual_condition_grade" :review="$comp->visual_review_required" compact />
                                <x-effective-grade-badge :grade="$comp->condition_grade" compact />
                            </div>
                            <p class="text-lg font-semibold tabular-nums text-emerald-700 dark:text-emerald-400">£{{ number_format((float) $comp->final_price, 2) }}</p>
                            <h3 class="mt-0.5 text-sm font-semibold leading-snug">{{ $comp->title }}</h3>
                            <p class="mt-1 truncate font-mono text-[11px] text-zinc-500">{{ $comp->model_key ?? 'unknown model key' }} · item {{ $comp->external_id }}</p>
                            <p class="mt-1 text-[11px] text-zinc-500">{{ str($comp->sale_type)->replace('_', ' ') }} · {{ $comp->bids_count ?? 0 }} bids {{ $comp->end_time ? '· sold '.$comp->end_time->format('j M Y') : '' }}</p>
                        </div>
                        <div class="flex flex-wrap items-center gap-3 sm:justify-end">
                            @if($comp->url)
                                <a href="{{ $comp->url }}" target="_blank" rel="noreferrer" class="text-xs font-medium text-zinc-500 underline decoration-zinc-300 underline-offset-4">Open sold row ↗</a>
                            @endif
                        </div>
                    </article>
                @empty
                    <div class="rounded-2xl border border-dashed border-zinc-300 p-7 text-center text-sm text-zinc-500 dark:border-zinc-700">No sold evidence matched this search.</div>
                @endforelse
            </div>
            @if($compCount > $comps->count())
                <p class="mt-3 text-xs text-zinc-500">Showing the first {{ number_format($comps->count()) }} sold evidence matches.</p>
            @endif
        </section>

        <section class="pb-8">
            <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
                <div>
                    <p class="text-xs font-bold uppercase tracking-[0.18em] text-rose-700 dark:text-rose-400">Saved by you</p>
                    <h2 class="text-4xl font-semibold tracking-tight">Favourites</h2>
                </div>
                <p class="max-w-lg text-sm text-zinc-500">Saved snapshots stay searchable even after a live auction disappears from the sync.</p>
            </div>
            <div class="grid gap-4">
                @forelse($favourites as $favourite)
                    <x-favourite-card :favourite="$favourite" />
                @empty
                    <div class="rounded-2xl border border-dashed border-zinc-300 p-7 text-center text-sm text-zinc-500 dark:border-zinc-700">No favourites matched this search.</div>
                @endforelse
            </div>
            @if($favouriteCount > $favourites->count())
                <p class="mt-3 text-xs text-zinc-500">Showing the first {{ number_format($favourites->count()) }} favourite matches.</p>
            @endif
        </section>
    @endif
</div>
