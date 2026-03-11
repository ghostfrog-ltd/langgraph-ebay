<article class="grid gap-6 rounded-xl border border-zinc-200 bg-transparent p-6 shadow-sm dark:border-zinc-800 lg:grid-cols-[minmax(0,1fr)_220px] lg:items-start">
    <div class="min-w-0">
        <div class="flex items-start justify-between gap-3">
            <h3 class="line-clamp-2 text-sm font-semibold leading-6 text-zinc-950 dark:text-white">
                {{ $listing->title }}
            </h3>
            <button
                type="button"
                wire:click="dismiss"
                class="shrink-0 rounded-md border border-zinc-300 px-2.5 py-1 text-xs font-medium text-zinc-600 transition hover:border-zinc-400 hover:text-zinc-950 dark:border-zinc-700 dark:text-zinc-300 dark:hover:border-zinc-500 dark:hover:text-white"
            >
                Dismiss
            </button>
        </div>

        <p class="mt-2 text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Reasoning</p>
        <p class="mt-1 line-clamp-3 text-sm leading-6 text-zinc-600 dark:text-zinc-300">
            {{ \Illuminate\Support\Str::limit($listing->llm_analysis ?: 'No analysis has been attached to this listing yet.', 240) }}
        </p>
    </div>

    <div class="flex flex-col gap-3 lg:items-end">
        <div class="space-y-1 lg:text-right">
            <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Current price</p>
            <p class="text-2xl font-semibold text-zinc-950 dark:text-white">£{{ number_format((float) $listing->current_price, 2) }}</p>
            <p class="text-sm font-medium text-emerald-600 dark:text-emerald-400">
                +£{{ number_format((float) ($listing->estimated_market_value - $listing->current_price), 2) }} profit
            </p>
            <p class="text-sm text-zinc-600 dark:text-zinc-300">{{ number_format($listing->roi_percentage, 2) }}% ROI</p>
        </div>

        <a
            href="{{ $listing->item_url }}"
            target="_blank"
            rel="noreferrer noopener"
            class="inline-flex items-center justify-center rounded-lg bg-zinc-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-zinc-800 dark:bg-white dark:text-zinc-950 dark:hover:bg-zinc-200"
        >
            View on eBay
        </a>
    </div>
</article>
