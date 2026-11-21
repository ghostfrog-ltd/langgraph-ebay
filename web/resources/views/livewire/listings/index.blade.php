<div class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7">
    <section class="grid grid-cols-2 overflow-hidden rounded-2xl border border-zinc-200 bg-white lg:grid-cols-4 dark:border-zinc-700 dark:bg-zinc-900">
        <div class="border-b border-r border-zinc-200 p-5 lg:border-b-0 dark:border-zinc-700"><p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Potentials</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($potentialCount) }}</p><p class="text-xs text-zinc-500">clear this gate</p></div>
        <div class="border-b border-zinc-200 p-5 lg:border-b-0 lg:border-r dark:border-zinc-700"><p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Matching</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($matchingCount) }}</p><p class="text-xs text-zinc-500">after filters</p></div>
        <div class="border-r border-zinc-200 p-5 dark:border-zinc-700"><p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Published</p><p class="mt-1 text-3xl font-semibold tabular-nums">{{ number_format($totalCount) }}</p><p class="text-xs text-zinc-500">decision-ready listings</p></div>
        <div class="p-5"><p class="text-[10px] font-bold uppercase tracking-wider text-zinc-500">Last sync</p><p class="mt-2 text-sm font-semibold">{{ $latestSync?->received_at?->diffForHumans() ?? 'Waiting for first sync' }}</p><p class="text-xs text-zinc-500">{{ $latestSync ? number_format($latestSync->comp_count).' evidence rows' : 'local → remote' }}</p></div>
    </section>

    @php
        $profitLabel = $profit === 25 ? 'Punt · £25-£49' : 'Source gates';
        $sortLabel = ['priority' => 'Priority', 'ending' => 'Ending', 'profit' => 'Profit'][$sort] ?? 'Priority';
        $saleTypeLabel = ['both' => 'Both types', 'bin' => 'BIN', 'auction' => 'Auction'][$saleType] ?? 'BIN';
        $visualLabel = ['all' => 'Visual all', 'checked' => 'Visual checked', 'unchecked' => 'Visual unchecked'][$visual] ?? 'Visual all';
        $textScanLabel = ['all' => 'Text scan all', 'checked' => 'Text checked', 'unchecked' => 'Text unchecked'][$textScan] ?? 'Text scan all';
        $sourceLabel = $source !== '' ? \App\Support\SourceCatalog::label($source) : 'All markets';
        $gradeLabel = $grade !== '' ? 'Grade '.$grade : 'All grades';
        $hasActiveFilters = $search !== '' || $source !== '' || $saleType !== 'bin' || $grade !== '' || $visual !== 'all' || $textScan !== 'all' || $profit !== 50 || $sort !== 'priority';
    @endphp

    <x-filter-drawer title="Buy desk filters" description="Tune the buy signal without the filter panel taking over the page.">
        <x-slot:summary>
            <div class="flex flex-wrap items-center gap-2 lg:justify-end">
                @foreach([$profitLabel, $sortLabel, $saleTypeLabel, $visualLabel, $textScanLabel, $sourceLabel, $gradeLabel] as $label)
                    <span class="rounded-full bg-zinc-100 px-3 py-1 text-xs font-semibold text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300">{{ $label }}</span>
                @endforeach
                @if($search !== '')
                    <span class="rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700 dark:bg-emerald-950 dark:text-lime-300">Search: {{ $search }}</span>
                @endif
                @if($hasActiveFilters)
                    <button type="button" wire:click="clearFilters" class="rounded-full px-3 py-1 text-xs font-bold text-zinc-500 underline underline-offset-4 hover:text-zinc-950 dark:hover:text-white">Clear</button>
                @endif
            </div>
        </x-slot:summary>

        <div class="space-y-2">
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Profit gate</p>
            <div class="grid grid-cols-2 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800">
                <button type="button" wire:click="chooseProfit(50)" class="rounded-lg px-4 py-3 text-sm font-bold transition {{ $profit === 50 ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Source gates</button>
                <button type="button" wire:click="chooseProfit(25)" class="rounded-lg px-4 py-3 text-sm font-bold transition {{ $profit === 25 ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Punt · £25-£49</button>
            </div>
        </div>

        <div class="space-y-2">
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Sort</p>
            <div class="grid grid-cols-3 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800" role="radiogroup" aria-label="Potential priority sort">
                <button type="button" wire:click="chooseSort('priority')" role="radio" aria-checked="{{ $sort === 'priority' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $sort === 'priority' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Priority</button>
                <button type="button" wire:click="chooseSort('ending')" role="radio" aria-checked="{{ $sort === 'ending' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $sort === 'ending' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Ending</button>
                <button type="button" wire:click="chooseSort('profit')" role="radio" aria-checked="{{ $sort === 'profit' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $sort === 'profit' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Profit</button>
            </div>
        </div>

        <div class="space-y-2">
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Type</p>
            <div class="grid grid-cols-3 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800" role="radiogroup" aria-label="Sale type filter">
                <button type="button" wire:click="chooseSaleType('both')" role="radio" aria-checked="{{ $saleType === 'both' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $saleType === 'both' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Both</button>
                <button type="button" wire:click="chooseSaleType('bin')" role="radio" aria-checked="{{ $saleType === 'bin' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $saleType === 'bin' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">BIN</button>
                <button type="button" wire:click="chooseSaleType('auction')" role="radio" aria-checked="{{ $saleType === 'auction' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $saleType === 'auction' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Auction</button>
            </div>
        </div>

        <div class="space-y-2">
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Visual</p>
            <div class="grid grid-cols-3 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800" role="radiogroup" aria-label="Visual check filter">
                <button type="button" wire:click="chooseVisual('all')" role="radio" aria-checked="{{ $visual === 'all' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $visual === 'all' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">All</button>
                <button type="button" wire:click="chooseVisual('checked')" role="radio" aria-checked="{{ $visual === 'checked' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $visual === 'checked' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Checked</button>
                <button type="button" wire:click="chooseVisual('unchecked')" role="radio" aria-checked="{{ $visual === 'unchecked' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $visual === 'unchecked' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Unchecked</button>
            </div>
        </div>

        <div class="space-y-2">
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Text scan</p>
            <div class="grid grid-cols-3 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800" role="radiogroup" aria-label="Text scan check filter">
                <button type="button" wire:click="chooseTextScan('all')" role="radio" aria-checked="{{ $textScan === 'all' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $textScan === 'all' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">All</button>
                <button type="button" wire:click="chooseTextScan('checked')" role="radio" aria-checked="{{ $textScan === 'checked' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $textScan === 'checked' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Checked</button>
                <button type="button" wire:click="chooseTextScan('unchecked')" role="radio" aria-checked="{{ $textScan === 'unchecked' ? 'true' : 'false' }}" class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $textScan === 'unchecked' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}">Unchecked</button>
            </div>
        </div>

        <flux:input wire:model.live.debounce.300ms="search" label="Search" placeholder="Try apple m1, RTX 4090, item ID" />

        <flux:select wire:model.live="source" label="Market">
            <flux:select.option value="">All markets</flux:select.option>
            @foreach($sources as $sourceOption)
                <flux:select.option value="{{ $sourceOption }}">{{ \App\Support\SourceCatalog::label($sourceOption) }}</flux:select.option>
            @endforeach
        </flux:select>

        <flux:select wire:model.live="grade" label="Grade">
            <flux:select.option value="">All grades</flux:select.option>
            @foreach($grades as $gradeOption)
                <flux:select.option value="{{ $gradeOption }}">Grade {{ $gradeOption }}</flux:select.option>
            @endforeach
        </flux:select>

        @if($hasActiveFilters)
            <flux:button wire:click="clearFilters" variant="ghost" class="w-full">Clear filters</flux:button>
        @endif
    </x-filter-drawer>

    <section>
        <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
            <div><p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-emerald-400">Above the line</p><h2 class="text-4xl font-semibold tracking-tight">Potentials</h2></div>
            <p class="max-w-lg text-sm text-zinc-500">Current price clears the selected source profit gate, using confirmed sold median and fixed cost assumptions.</p>
        </div>
        <div class="grid gap-4">
            @forelse($potentials as $listing)
                <x-listing-card :listing="$listing" />
            @empty
                <div class="rounded-2xl border border-dashed border-zinc-300 p-10 text-center dark:border-zinc-700">
                    <p class="text-2xl font-semibold">No listings clear this gate right now.</p>
                    <p class="mt-2 text-sm text-zinc-500">The closest evidence-backed misses are shown below.</p>
                </div>
            @endforelse
        </div>
        <div class="mt-5">{{ $potentials->links() }}</div>
    </section>

    <section class="pb-8">
        <div class="mb-5 flex flex-col justify-between gap-2 sm:flex-row sm:items-end">
            <div><p class="text-xs font-bold uppercase tracking-[0.18em] text-amber-700 dark:text-amber-400">Within £10</p><h2 class="text-4xl font-semibold tracking-tight">Closest misses</h2></div>
            <p class="max-w-lg text-sm text-zinc-500">These miss the selected gate by a small amount. The site shows the maths; the decision stays human.</p>
        </div>
        <div class="grid gap-4">
            @forelse($nearMisses as $listing)
                <x-listing-card :listing="$listing" :near="true" />
            @empty
                <div class="rounded-2xl border border-dashed border-zinc-300 p-7 text-center text-sm text-zinc-500 dark:border-zinc-700">No useful near misses for these filters.</div>
            @endforelse
        </div>
    </section>
</div>
