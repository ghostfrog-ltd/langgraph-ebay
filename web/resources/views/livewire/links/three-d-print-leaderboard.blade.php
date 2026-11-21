<div class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7">
    <section class="grid grid-cols-2 overflow-hidden rounded-2xl border border-zinc-200 bg-white lg:grid-cols-4 dark:border-zinc-700 dark:bg-zinc-900">
        <div class="border-b border-r border-zinc-200 p-5 lg:border-b-0 dark:border-zinc-700">
            <p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Business sellers</p>
            <p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($sellerCount) }}</p>
            <p class="text-xs text-zinc-500">active 3D print matches</p>
        </div>
        <div class="border-b border-zinc-200 p-5 lg:border-b-0 lg:border-r dark:border-zinc-700">
            <p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Sampled items</p>
            <p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($itemCount) }}</p>
            <p class="text-xs text-zinc-500">deduped by item ID</p>
        </div>
        <div class="border-r border-zinc-200 p-5 dark:border-zinc-700">
            <p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Active value</p>
            <p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format((float) $activeValue, 0) }}</p>
            <p class="text-xs text-zinc-500">visible sampled stock</p>
        </div>
        <div class="p-5">
            <p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Last refresh</p>
            <p class="mt-2 text-sm font-semibold">
                {{ $latestSnapshot ? \Illuminate\Support\Carbon::parse($latestSnapshot)->diffForHumans() : 'Waiting for first sync' }}
            </p>
            <p class="text-xs text-zinc-500">eBay GB business sellers</p>
        </div>
    </section>

    <section>
        <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
            <div>
                <p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-emerald-400">Seller research</p>
                <h1 class="text-4xl font-semibold tracking-tight">3D print leaderboard</h1>
            </div>
            <p class="max-w-xl text-sm text-zinc-500">Ranked from a daily eBay Browse API snapshot of active GB business listings. This measures visible inventory depth, not confirmed revenue.</p>
        </div>

        <div class="overflow-hidden rounded-2xl border border-zinc-200 bg-white shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
            <div class="overflow-x-auto">
                <table class="min-w-full divide-y divide-zinc-200 text-sm dark:divide-zinc-700">
                    <thead class="bg-zinc-50 text-left text-[10px] font-bold uppercase tracking-wider text-zinc-500 dark:bg-zinc-800/70">
                        <tr>
                            <th class="px-4 py-3">Rank</th>
                            <th class="px-4 py-3">Seller</th>
                            <th class="px-4 py-3 text-right">Items</th>
                            <th class="px-4 py-3 text-right">Active value</th>
                            <th class="px-4 py-3 text-right">Max price</th>
                            <th class="px-4 py-3">Feedback</th>
                            <th class="px-4 py-3">Matched niches</th>
                            <th class="px-4 py-3">Examples</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-zinc-100 dark:divide-zinc-800">
                        @forelse($sellers as $seller)
                            <tr class="align-top hover:bg-zinc-50/80 dark:hover:bg-zinc-800/50">
                                <td class="whitespace-nowrap px-4 py-4 font-semibold tabular-nums text-zinc-500">#{{ $seller->rank }}</td>
                                <td class="whitespace-nowrap px-4 py-4">
                                    <a href="{{ $seller->sellerUrl() }}" target="_blank" rel="noopener noreferrer" class="font-semibold text-emerald-700 hover:text-emerald-900 dark:text-emerald-400 dark:hover:text-emerald-300">
                                        {{ $seller->seller_username }}
                                    </a>
                                </td>
                                <td class="whitespace-nowrap px-4 py-4 text-right font-semibold tabular-nums">{{ number_format($seller->item_count) }}</td>
                                <td class="whitespace-nowrap px-4 py-4 text-right tabular-nums">£{{ number_format((float) $seller->active_value, 2) }}</td>
                                <td class="whitespace-nowrap px-4 py-4 text-right tabular-nums">£{{ number_format((float) $seller->max_price, 2) }}</td>
                                <td class="whitespace-nowrap px-4 py-4">
                                    <span class="font-medium tabular-nums">{{ number_format($seller->feedback_score) }}</span>
                                    <span class="text-zinc-500">/ {{ number_format((float) $seller->feedback_percentage, 1) }}%</span>
                                </td>
                                <td class="min-w-56 px-4 py-4 text-zinc-600 dark:text-zinc-300">
                                    {{ collect($seller->matched_queries)->take(5)->implode(', ') }}
                                </td>
                                <td class="min-w-80 px-4 py-4">
                                    <div class="space-y-2">
                                        @foreach(collect($seller->example_titles)->take(3) as $title)
                                            <p class="line-clamp-2 text-zinc-700 dark:text-zinc-200">{{ $title }}</p>
                                        @endforeach
                                        @if($seller->example_url)
                                            <a href="{{ $seller->example_url }}" target="_blank" rel="noopener noreferrer" class="inline-flex text-xs font-semibold text-emerald-700 hover:text-emerald-900 dark:text-emerald-400 dark:hover:text-emerald-300">Example listing</a>
                                        @endif
                                    </div>
                                </td>
                            </tr>
                        @empty
                            <tr>
                                <td colspan="8" class="px-4 py-12 text-center">
                                    <p class="text-2xl font-semibold">No leaderboard snapshot yet.</p>
                                    <p class="mt-2 text-sm text-zinc-500">The daily sync will populate this page after the first eBay refresh.</p>
                                </td>
                            </tr>
                        @endforelse
                    </tbody>
                </table>
            </div>
        </div>
    </section>
</div>
