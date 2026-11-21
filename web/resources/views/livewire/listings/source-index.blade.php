<div class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7">
    <section class="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="flex flex-col justify-between gap-3 sm:flex-row sm:items-end">
            <div>
                <p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-emerald-400">Source listings</p>
                <h1 class="text-3xl font-semibold tracking-tight">{{ $sourceLabel }}</h1>
            </div>
            <p class="max-w-lg text-sm text-zinc-500">
                {{ number_format($listingCount) }} listings, ordered by ending soonest.
                @if($gate === 'clears')
                    Showing rows with expected profit of at least £{{ number_format($profitGate) }}.
                @elseif(\App\Support\SourceCatalog::requiresComps($source))
                    Rows without sold evidence or a safe buy price are hidden.
                @else
                    Motomine can show rows before sold evidence is attached; negative safe-buy rows are hidden.
                @endif
            </p>
        </div>
    </section>

    @php
        $gateLabel = $gate === 'all' ? 'All listings' : 'Clears £'.number_format($profitGate);
        $saleTypeLabel = ['both' => 'Both types', 'bin' => 'BIN', 'auction' => 'Auction'][$saleType] ?? 'BIN';
        $visualLabel = ['all' => 'Visual all', 'checked' => 'Visual checked', 'unchecked' => 'Visual unchecked'][$visual] ?? 'Visual all';
        $textScanLabel = ['all' => 'Text scan all', 'checked' => 'Text checked', 'unchecked' => 'Text unchecked'][$textScan] ?? 'Text scan all';
        $gradeLabel = $grade !== '' ? 'Grade '.$grade : 'All grades';
        $hasActiveFilters = $search !== '' || $gate !== 'clears' || $saleType !== 'bin' || $grade !== '' || $visual !== 'all' || $textScan !== 'all';
    @endphp

    <x-filter-drawer title="{{ $sourceLabel }} filters" description="Narrow this source list without losing the listings underneath.">
        <x-slot:summary>
            <div class="flex flex-wrap items-center gap-2 lg:justify-end">
                @foreach([$gateLabel, $saleTypeLabel, $visualLabel, $textScanLabel, $gradeLabel] as $label)
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
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Gate</p>
            <div class="grid grid-cols-2 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800" role="radiogroup" aria-label="Listing gate filter">
                <button
                    type="button"
                    wire:click="chooseGate('clears')"
                    role="radio"
                    aria-checked="{{ $gate === 'clears' ? 'true' : 'false' }}"
                    class="rounded-lg px-4 py-3 text-sm font-bold transition {{ $gate === 'clears' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    Clears gate
                </button>
                <button
                    type="button"
                    wire:click="chooseGate('all')"
                    role="radio"
                    aria-checked="{{ $gate === 'all' ? 'true' : 'false' }}"
                    class="rounded-lg px-4 py-3 text-sm font-bold transition {{ $gate === 'all' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    All
                </button>
            </div>
        </div>

        <div class="space-y-2">
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Type</p>
            <div class="grid grid-cols-3 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800" role="radiogroup" aria-label="Sale type filter">
                <button
                    type="button"
                    wire:click="chooseSaleType('both')"
                    role="radio"
                    aria-checked="{{ $saleType === 'both' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $saleType === 'both' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    Both
                </button>
                <button
                    type="button"
                    wire:click="chooseSaleType('bin')"
                    role="radio"
                    aria-checked="{{ $saleType === 'bin' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $saleType === 'bin' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    BIN
                </button>
                <button
                    type="button"
                    wire:click="chooseSaleType('auction')"
                    role="radio"
                    aria-checked="{{ $saleType === 'auction' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $saleType === 'auction' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    Auction
                </button>
            </div>
        </div>

        <div class="space-y-2">
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Visual</p>
            <div class="grid grid-cols-3 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800" role="radiogroup" aria-label="Visual check filter">
                <button
                    type="button"
                    wire:click="chooseVisual('all')"
                    role="radio"
                    aria-checked="{{ $visual === 'all' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $visual === 'all' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    All
                </button>
                <button
                    type="button"
                    wire:click="chooseVisual('checked')"
                    role="radio"
                    aria-checked="{{ $visual === 'checked' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $visual === 'checked' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    Checked
                </button>
                <button
                    type="button"
                    wire:click="chooseVisual('unchecked')"
                    role="radio"
                    aria-checked="{{ $visual === 'unchecked' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $visual === 'unchecked' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    Unchecked
                </button>
            </div>
        </div>

        <div class="space-y-2">
            <p class="text-sm font-medium text-zinc-950 dark:text-white">Text scan</p>
            <div class="grid grid-cols-3 rounded-xl bg-zinc-100 p-1 dark:bg-zinc-800" role="radiogroup" aria-label="Text scan check filter">
                <button
                    type="button"
                    wire:click="chooseTextScan('all')"
                    role="radio"
                    aria-checked="{{ $textScan === 'all' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $textScan === 'all' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    All
                </button>
                <button
                    type="button"
                    wire:click="chooseTextScan('checked')"
                    role="radio"
                    aria-checked="{{ $textScan === 'checked' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $textScan === 'checked' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    Checked
                </button>
                <button
                    type="button"
                    wire:click="chooseTextScan('unchecked')"
                    role="radio"
                    aria-checked="{{ $textScan === 'unchecked' ? 'true' : 'false' }}"
                    class="rounded-lg px-3 py-3 text-sm font-bold transition {{ $textScan === 'unchecked' ? 'bg-zinc-950 text-white dark:bg-white dark:text-zinc-950' : 'text-zinc-600 hover:text-zinc-950 dark:text-zinc-400 dark:hover:text-white' }}"
                >
                    Unchecked
                </button>
            </div>
        </div>

        <flux:input wire:model.live.debounce.300ms="search" label="Search {{ $sourceLabel }}" placeholder="Try apple m1, 256gb, item ID" />

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
        <div class="grid gap-4">
            @forelse($listings as $listing)
                <x-listing-card :listing="$listing" mode="source" :profit-gate="$profitGate" />
            @empty
                <div class="rounded-2xl border border-dashed border-zinc-300 p-10 text-center dark:border-zinc-700">
                    <p class="text-2xl font-semibold">No listings for this source yet.</p>
                </div>
            @endforelse
        </div>
        <div class="mt-5">{{ $listings->links() }}</div>
    </section>
</div>
