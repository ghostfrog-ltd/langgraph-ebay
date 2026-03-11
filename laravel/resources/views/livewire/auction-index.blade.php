@php
    $statusClasses = [
        'ingested' => 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-200',
        'analyzing' => 'bg-sky-100 text-sky-700 dark:bg-sky-950/50 dark:text-sky-300',
        'opportunity' => 'bg-emerald-100 text-emerald-700 dark:bg-emerald-950/50 dark:text-emerald-300',
        'dismissed' => 'bg-zinc-200 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400',
    ];
@endphp

<div
    x-data="{
        keyword: '',
        minPrice: '',
        maxPrice: '',
        source: '',
        category: '',
        selectedIds: @entangle('selectedIds'),
        matches(row) {
            const keyword = this.keyword.trim().toLowerCase();
            const title = (row.dataset.title || '').toLowerCase();
            const source = row.dataset.source || '';
            const category = row.dataset.category || '';
            const price = parseFloat(row.dataset.price || '0');

            if (keyword && !title.includes(keyword)) return false;
            if (this.source && source !== this.source) return false;
            if (this.category && category !== this.category) return false;
            if (this.minPrice !== '' && price < parseFloat(this.minPrice)) return false;
            if (this.maxPrice !== '' && price > parseFloat(this.maxPrice)) return false;

            return true;
        },
        toggleAll(event) {
            if (event.target.checked) {
                this.selectedIds = Array.from(document.querySelectorAll('[data-auction-checkbox]'))
                    .map((checkbox) => Number(checkbox.value));
                return;
            }
            this.selectedIds = [];
        },
    }"
    class="flex h-full flex-1 flex-col gap-6"
>
    <section class="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div>
                <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Discovery</p>
                <h1 class="mt-2 text-2xl font-semibold text-zinc-950 dark:text-white">Auction Feed</h1>
                <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">Raw synchronized listings with local instant filtering and bulk triage.</p>
            </div>

            <div class="flex flex-wrap items-center gap-2">
                <button
                    type="button"
                    wire:click="bulkDismiss"
                    :disabled="selectedIds.length === 0"
                    class="rounded-lg border border-zinc-300 px-3 py-2 text-sm font-medium text-zinc-700 transition disabled:cursor-not-allowed disabled:opacity-40 dark:border-zinc-700 dark:text-zinc-200"
                >
                    Bulk Dismiss
                </button>
                <button
                    type="button"
                    wire:click="bulkReanalyze"
                    :disabled="selectedIds.length === 0"
                    class="rounded-lg bg-zinc-950 px-3 py-2 text-sm font-medium text-white transition disabled:cursor-not-allowed disabled:opacity-40 dark:bg-white dark:text-zinc-950"
                >
                    Bulk Re-analyze
                </button>
            </div>
        </div>

        <div class="mt-4 flex items-center justify-between gap-4 text-sm text-zinc-600 dark:text-zinc-300">
            <p>Showing {{ $listings->count() }} of {{ number_format($listings->total()) }} listings on this page.</p>

            <label class="flex items-center gap-2">
                <span class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Per page</span>
                <select wire:model.live="perPage" class="rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-950 outline-none transition focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-950 dark:text-white">
                    <option value="100">100</option>
                    <option value="250">250</option>
                    <option value="500">500</option>
                </select>
            </label>
        </div>

        <div class="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-5">
            <div class="xl:col-span-2">
                <label class="mb-1 block text-xs font-medium uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Keyword</label>
                <input x-model="keyword" type="text" placeholder="Search title..." class="w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-950 outline-none transition focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-950 dark:text-white">
            </div>

            <div>
                <label class="mb-1 block text-xs font-medium uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Source</label>
                <select x-model="source" class="w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-950 outline-none transition focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-950 dark:text-white">
                    <option value="">All sources</option>
                    @foreach ($sourcePlatforms as $sourcePlatform)
                        <option value="{{ $sourcePlatform }}">{{ $sourcePlatform }}</option>
                    @endforeach
                </select>
            </div>

            <div>
                <label class="mb-1 block text-xs font-medium uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Category</label>
                <select x-model="category" class="w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-950 outline-none transition focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-950 dark:text-white">
                    <option value="">All categories</option>
                    @foreach ($categoryNames as $categoryName)
                        <option value="{{ $categoryName }}">{{ $categoryName }}</option>
                    @endforeach
                </select>
            </div>

            <div class="grid grid-cols-2 gap-3">
                <div>
                    <label class="mb-1 block text-xs font-medium uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Min</label>
                    <input x-model="minPrice" type="number" step="0.01" placeholder="0" class="w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-950 outline-none transition focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-950 dark:text-white">
                </div>
                <div>
                    <label class="mb-1 block text-xs font-medium uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Max</label>
                    <input x-model="maxPrice" type="number" step="0.01" placeholder="5000" class="w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-950 outline-none transition focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-950 dark:text-white">
                </div>
            </div>
        </div>
    </section>

    <section class="overflow-hidden rounded-xl border border-zinc-200 bg-white shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="overflow-x-auto">
            <table class="min-w-full text-left text-sm">
                <thead class="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-800 dark:bg-zinc-950/60">
                    <tr>
                        <th class="px-4 py-3">
                            <input type="checkbox" @change="toggleAll($event)" class="h-4 w-4 rounded border-zinc-300 text-zinc-950 focus:ring-zinc-500 dark:border-zinc-700 dark:bg-zinc-900">
                        </th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Item</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Source</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Status</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Current</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Market</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Scraped</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-zinc-200 dark:divide-zinc-800">
                    @foreach ($listings as $listing)
                        <tr
                            x-show="matches($el)"
                            data-title="{{ strtolower($listing->title) }}"
                            data-source="{{ $listing->source_platform }}"
                            data-category="{{ $listing->category_name }}"
                            data-price="{{ $listing->current_price }}"
                            wire:key="auction-row-{{ $listing->id }}"
                            wire:click="openListing({{ $listing->id }})"
                            class="cursor-pointer transition hover:bg-zinc-50 dark:hover:bg-zinc-950/60"
                        >
                            <td class="px-4 py-3" @click.stop>
                                <input
                                    data-auction-checkbox
                                    type="checkbox"
                                    value="{{ $listing->id }}"
                                    x-model="selectedIds"
                                    class="h-4 w-4 rounded border-zinc-300 text-zinc-950 focus:ring-zinc-500 dark:border-zinc-700 dark:bg-zinc-900"
                                >
                            </td>
                            <td class="px-4 py-3">
                                <div class="flex items-center gap-3">
                                    <div class="overflow-hidden rounded-lg border border-zinc-200 bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-800">
                                        @if ($listing->image_url)
                                            <img src="{{ $listing->image_url }}" alt="{{ $listing->title }}" class="h-12 w-12 object-cover">
                                        @else
                                            <div class="flex h-12 w-12 items-center justify-center text-[10px] uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">N/A</div>
                                        @endif
                                    </div>
                                    <div class="min-w-0">
                                        <p class="line-clamp-2 font-medium text-zinc-950 dark:text-white">{{ $listing->title }}</p>
                                        <p class="mt-1 text-xs text-zinc-500 dark:text-zinc-400">{{ $listing->category_name ?: 'Uncategorized' }}</p>
                                    </div>
                                </div>
                            </td>
                            <td class="px-4 py-3 text-zinc-600 dark:text-zinc-300">
                                <div>{{ $listing->source_platform ?: 'Unknown source' }}</div>
                                <div class="mt-1 text-xs text-zinc-500 dark:text-zinc-400">{{ $listing->ebay_id }}</div>
                            </td>
                            <td class="px-4 py-3">
                                <span class="inline-flex rounded-full px-2.5 py-1 text-xs font-medium {{ $statusClasses[$listing->pipeline_state] ?? $statusClasses['ingested'] }}">
                                    {{ ucfirst($listing->pipeline_state) }}
                                </span>
                            </td>
                            <td class="px-4 py-3 font-semibold text-zinc-950 dark:text-white">£{{ number_format((float) $listing->current_price, 2) }}</td>
                            <td class="px-4 py-3 text-zinc-600 dark:text-zinc-300">£{{ number_format((float) $listing->estimated_market_value, 2) }}</td>
                            <td class="px-4 py-3 text-zinc-600 dark:text-zinc-300">
                                @php
                                    $timeLeftSeconds = data_get($listing->attributes_json, 'time_left_seconds');
                                @endphp
                                @if ($timeLeftSeconds)
                                    <div class="font-medium">{{ \Carbon\CarbonInterval::seconds((int) $timeLeftSeconds)->cascade()->forHumans(['short' => true]) }}</div>
                                    <div class="mt-1 text-xs text-zinc-500 dark:text-zinc-400">Time left</div>
                                @else
                                    <div class="font-medium">{{ $listing->updated_at?->format('d M Y') }}</div>
                                    <div class="mt-1 text-xs text-zinc-500 dark:text-zinc-400">{{ $listing->updated_at?->format('H:i:s') }}</div>
                                @endif
                            </td>
                        </tr>
                    @endforeach
                </tbody>
            </table>
        </div>

        <div class="border-t border-zinc-200 px-4 py-3 dark:border-zinc-800">
            {{ $listings->links() }}
        </div>
    </section>

    @if ($selectedListing)
        <div class="fixed inset-0 z-40 bg-zinc-950/40" wire:click="closeListing"></div>

        <aside class="fixed inset-y-0 right-0 z-50 flex w-full max-w-2xl flex-col border-l border-zinc-200 bg-white shadow-2xl dark:border-zinc-700 dark:bg-zinc-950">
            <div class="flex items-start justify-between gap-4 border-b border-zinc-200 px-6 py-5 dark:border-zinc-800">
                <div>
                    <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Quick Look</p>
                    <h2 class="mt-2 text-lg font-semibold text-zinc-950 dark:text-white">{{ $selectedListing->title }}</h2>
                    <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">{{ $selectedListing->source_platform ?: 'Unknown source' }} • {{ $selectedListing->category_name ?: 'Uncategorized' }}</p>
                </div>
                <button type="button" wire:click="closeListing" class="rounded-md border border-zinc-300 px-3 py-1.5 text-sm text-zinc-600 dark:border-zinc-700 dark:text-zinc-300">Close</button>
            </div>

            <div class="flex-1 overflow-y-auto px-6 py-5">
                <div class="grid gap-5 md:grid-cols-2">
                    <div class="space-y-3">
                        <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-900">
                            <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Current price</p>
                            <p class="mt-2 text-2xl font-semibold text-zinc-950 dark:text-white">£{{ number_format((float) $selectedListing->current_price, 2) }}</p>
                        </div>
                        <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-900">
                            <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Estimated market</p>
                            <p class="mt-2 text-2xl font-semibold text-zinc-950 dark:text-white">£{{ number_format((float) $selectedListing->estimated_market_value, 2) }}</p>
                        </div>
                        <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-900">
                            <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Scraped at</p>
                            <p class="mt-2 text-sm font-medium text-zinc-950 dark:text-white">{{ $selectedListing->created_at?->format('d M Y H:i:s') }}</p>
                        </div>
                    </div>

                    <div class="space-y-3">
                        <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-900">
                            <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Status</p>
                            <p class="mt-2 text-sm font-medium text-zinc-950 dark:text-white">{{ ucfirst($selectedListing->pipeline_state) }}</p>
                        </div>
                        <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-900">
                            <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Analysis</p>
                            <p class="mt-2 text-sm leading-6 text-zinc-600 dark:text-zinc-300">{{ $selectedListing->llm_analysis ?: 'No LLM analysis stored.' }}</p>
                        </div>
                    </div>
                </div>

                <div class="mt-6">
                    <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Attributes JSON</p>
                    <pre class="mt-3 overflow-x-auto rounded-xl border border-zinc-200 bg-zinc-50 p-4 text-xs leading-6 text-zinc-700 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-200">{{ json_encode($selectedListing->attributes_json ?? [], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) }}</pre>
                </div>
            </div>
        </aside>
    @endif
</div>
