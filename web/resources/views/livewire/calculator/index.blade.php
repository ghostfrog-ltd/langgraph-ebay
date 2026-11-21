<div class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7">
    <section class="grid gap-4 lg:grid-cols-[minmax(0,1fr)_360px]">
        <div class="rounded-2xl bg-zinc-950 p-6 text-white shadow-[7px_7px_0_#bef264] sm:p-8">
            <div class="flex flex-col gap-5 lg:flex-row lg:items-end">
                <div class="flex-1">
                    <p class="text-xs font-bold uppercase tracking-[0.18em] text-lime-300">Sale calculator</p>
                    <h1 class="mt-1 text-4xl font-semibold tracking-tight sm:text-5xl">Net after fees</h1>
                </div>
                <label class="block min-w-0 lg:w-72">
                    <span class="text-xs font-bold uppercase tracking-widest text-zinc-400">Sell price</span>
                    <span class="mt-2 flex items-center rounded-xl bg-white px-4 text-zinc-950 shadow-sm focus-within:ring-2 focus-within:ring-lime-300">
                        <span class="text-2xl font-semibold">£</span>
                        <input
                            type="number"
                            min="0"
                            step="0.01"
                            inputmode="decimal"
                            wire:model.live.debounce.150ms="sellPrice"
                            class="min-w-0 flex-1 bg-transparent py-3 pl-2 text-4xl font-semibold tabular-nums outline-none"
                        >
                    </span>
                </label>
            </div>
        </div>

        <div class="rounded-2xl border border-zinc-200 bg-white p-6 dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">{{ $math['ownedItem'] ? 'Take-home' : 'Expected profit' }}</p>
            <p class="mt-1 text-5xl font-semibold tabular-nums text-emerald-700 dark:text-emerald-400">£{{ number_format($math['net'], 2) }}</p>
            <p class="mt-2 text-sm text-zinc-500">Total deductions £{{ number_format($math['totalCosts'], 2) }}{{ $math['roi'] > 0 ? ' · '.number_format($math['roi'], 1).'% ROI' : '' }}</p>
        </div>
    </section>

    <section class="grid gap-4 lg:grid-cols-4">
        <div class="rounded-2xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Sell price</p>
            <p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format($math['sellPrice'], 2) }}</p>
        </div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Fees</p>
            <p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format($math['fees'], 2) }}</p>
        </div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Fixed costs</p>
            <p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format($math['totalCosts'] - $math['fees'] - $math['buyPrice'], 2) }}</p>
        </div>
        <div class="rounded-2xl border border-zinc-200 bg-white p-4 dark:border-zinc-700 dark:bg-zinc-900">
            <p class="text-[10px] font-bold uppercase tracking-widest text-zinc-500">Buy price</p>
            <p class="mt-1 text-3xl font-semibold tabular-nums">£{{ number_format($math['buyPrice'], 2) }}</p>
        </div>
    </section>

    <section class="grid items-start gap-8 lg:grid-cols-[minmax(0,1fr)_380px]">
        <div class="rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-700 dark:bg-zinc-900">
            <div class="grid gap-4 sm:grid-cols-2">
                <label class="block">
                    <span class="text-xs font-bold uppercase tracking-wider text-zinc-500">eBay fee %</span>
                    <input type="number" min="0" step="0.1" inputmode="decimal" wire:model.live.debounce.150ms="feeRate" class="mt-2 w-full rounded-xl border border-zinc-200 bg-white px-3 py-2.5 text-lg tabular-nums dark:border-zinc-700 dark:bg-zinc-950">
                </label>
                <label class="block">
                    <span class="text-xs font-bold uppercase tracking-wider text-zinc-500">Outbound postage</span>
                    <input type="number" min="0" step="0.01" inputmode="decimal" wire:model.live.debounce.150ms="outboundPostage" class="mt-2 w-full rounded-xl border border-zinc-200 bg-white px-3 py-2.5 text-lg tabular-nums dark:border-zinc-700 dark:bg-zinc-950">
                </label>
                <label class="block">
                    <span class="text-xs font-bold uppercase tracking-wider text-zinc-500">Inbound postage</span>
                    <input type="number" min="0" step="0.01" inputmode="decimal" wire:model.live.debounce.150ms="inboundPostage" @disabled($ownedItem) class="mt-2 w-full rounded-xl border border-zinc-200 bg-white px-3 py-2.5 text-lg tabular-nums disabled:opacity-40 dark:border-zinc-700 dark:bg-zinc-950">
                </label>
                <label class="block">
                    <span class="text-xs font-bold uppercase tracking-wider text-zinc-500">Packaging</span>
                    <input type="number" min="0" step="0.01" inputmode="decimal" wire:model.live.debounce.150ms="packaging" class="mt-2 w-full rounded-xl border border-zinc-200 bg-white px-3 py-2.5 text-lg tabular-nums dark:border-zinc-700 dark:bg-zinc-950">
                </label>
                <label class="block">
                    <span class="text-xs font-bold uppercase tracking-wider text-zinc-500">Selling reserve</span>
                    <input type="number" min="0" step="0.01" inputmode="decimal" wire:model.live.debounce.150ms="sellingReserve" class="mt-2 w-full rounded-xl border border-zinc-200 bg-white px-3 py-2.5 text-lg tabular-nums dark:border-zinc-700 dark:bg-zinc-950">
                </label>
                <label class="block">
                    <span class="text-xs font-bold uppercase tracking-wider text-zinc-500">Buy price</span>
                    <input type="number" min="0" step="0.01" inputmode="decimal" wire:model.live.debounce.150ms="buyPrice" @disabled($ownedItem) class="mt-2 w-full rounded-xl border border-zinc-200 bg-white px-3 py-2.5 text-lg tabular-nums disabled:opacity-40 dark:border-zinc-700 dark:bg-zinc-950">
                </label>
            </div>

            <div class="mt-5 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <label class="inline-flex items-center gap-3 text-sm font-semibold">
                    <input type="checkbox" wire:model.live="ownedItem" class="size-5 rounded border-zinc-300 text-emerald-700 focus:ring-lime-300">
                    I already own it
                </label>
                <flux:button wire:click="resetDefaults" variant="ghost">Reset defaults</flux:button>
            </div>
        </div>

        <aside class="rounded-2xl bg-zinc-950 p-6 text-white shadow-[7px_7px_0_#bef264] lg:sticky lg:top-5">
            <p class="text-xs font-bold uppercase tracking-[0.18em] text-lime-300">The maths</p>
            <h2 class="mt-1 text-3xl font-semibold tracking-tight">From sale to net</h2>
            <dl class="mt-5 divide-y divide-zinc-700 text-sm">
                <div class="flex justify-between py-3"><dt>Sell price</dt><dd class="font-bold">£{{ number_format($math['sellPrice'], 2) }}</dd></div>
                <div class="flex justify-between py-3 text-zinc-400"><dt>eBay fees · {{ number_format($math['feeRate'], 1) }}%</dt><dd>- £{{ number_format($math['fees'], 2) }}</dd></div>
                <div class="flex justify-between py-3 text-zinc-400"><dt>Outbound postage</dt><dd>- £{{ number_format($math['outboundPostage'], 2) }}</dd></div>
                @unless($math['ownedItem'])
                    <div class="flex justify-between py-3 text-zinc-400"><dt>Inbound postage</dt><dd>- £{{ number_format($math['inboundPostage'], 2) }}</dd></div>
                @endunless
                <div class="flex justify-between py-3 text-zinc-400"><dt>Packaging</dt><dd>- £{{ number_format($math['packaging'], 2) }}</dd></div>
                <div class="flex justify-between py-3 text-zinc-400"><dt>Selling reserve</dt><dd>- £{{ number_format($math['sellingReserve'], 2) }}</dd></div>
                @unless($math['ownedItem'])
                    <div class="flex justify-between py-3 text-zinc-400"><dt>Buy price</dt><dd>- £{{ number_format($math['buyPrice'], 2) }}</dd></div>
                @endunless
                <div class="flex justify-between py-4 text-lg font-semibold tabular-nums text-lime-300"><dt>{{ $math['ownedItem'] ? 'Net to you' : 'Expected profit' }}</dt><dd>£{{ number_format($math['net'], 2) }}</dd></div>
            </dl>
        </aside>
    </section>
</div>
