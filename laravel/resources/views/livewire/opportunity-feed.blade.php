<div wire:poll.10s class="flex h-full w-full flex-1 flex-col gap-6">
    <section class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <div class="rounded-xl border border-zinc-200 bg-white p-4 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Listings</p>
            <p class="mt-3 text-3xl font-semibold text-zinc-950 dark:text-white">{{ number_format($allListingsCount) }}</p>
            <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">{{ number_format($recentlySyncedCount) }} synced in the last hour.</p>
        </div>

        <div class="rounded-xl border border-zinc-200 bg-white p-4 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Pipeline</p>
            <p class="mt-3 text-3xl font-semibold text-zinc-950 dark:text-white">{{ $opportunities->count() }}</p>
            <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">Hot opportunities ready for review.</p>
        </div>

        <div class="rounded-xl border border-zinc-200 bg-white p-4 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Best ROI</p>
            <p class="mt-3 text-3xl font-semibold text-zinc-950 dark:text-white">
                {{ number_format((float) ($opportunities->max('roi_percentage') ?? 0), 2) }}%
            </p>
            <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">Highest upside currently in the feed.</p>
        </div>

        <div class="rounded-xl border border-zinc-200 bg-white p-4 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Projected Gain</p>
            <p class="mt-3 text-3xl font-semibold text-emerald-600 dark:text-emerald-400">
                £{{ number_format((float) $opportunities->sum(fn ($listing) => $listing->estimated_market_value - $listing->current_price), 2) }}
            </p>
            <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">Combined spread across visible opportunities.</p>
        </div>
    </section>

    <section class="rounded-xl border border-zinc-200 bg-white shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="px-5 py-4">
            <h2 class="text-lg font-semibold text-zinc-950 dark:text-white">Opportunity Feed</h2>
            <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">Highest-ROI opportunities surfaced by the pipeline.</p>
        </div>

        <div class="mt-4 space-y-4 px-5 pb-5 pt-6">
            @forelse ($opportunities as $listing)
                <livewire:opportunity-card :listing="$listing" :key="$listing->id" />
            @empty
                <div class="py-10 text-sm text-zinc-600 dark:text-zinc-300">
                    No hot opportunities are currently available.
                </div>
            @endforelse
        </div>
    </section>
</div>
