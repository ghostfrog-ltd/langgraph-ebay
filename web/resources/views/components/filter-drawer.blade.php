@props([
    'title' => 'Filters',
    'description' => null,
])

<div
    x-data="{ filtersOpen: false }"
    x-on:keydown.escape.window="filtersOpen = false"
>
    <section class="rounded-2xl border border-zinc-200 bg-white p-4 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <button
                type="button"
                x-on:click="filtersOpen = true"
                class="inline-flex w-fit items-center gap-2 rounded-xl bg-zinc-950 px-4 py-2.5 text-sm font-bold text-white transition hover:bg-zinc-800 dark:bg-white dark:text-zinc-950 dark:hover:bg-zinc-200"
                aria-label="Open filters"
            >
                <svg class="size-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
                    <path stroke-linecap="round" d="M4 7h16M4 12h16M4 17h16" />
                </svg>
                Filters
            </button>

            @isset($summary)
                <div class="min-w-0 flex-1 lg:text-right">
                    {{ $summary }}
                </div>
            @endisset
        </div>
    </section>

    <div
        x-cloak
        x-show="filtersOpen"
        class="fixed inset-0 z-50"
        role="dialog"
        aria-modal="true"
        aria-label="{{ $title }}"
    >
        <div
            x-show="filtersOpen"
            x-transition.opacity
            class="absolute inset-0 bg-zinc-950/50 backdrop-blur-sm"
            x-on:click="filtersOpen = false"
        ></div>

        <aside
            x-show="filtersOpen"
            x-transition:enter="transition ease-out duration-200"
            x-transition:enter-start="translate-x-full opacity-90"
            x-transition:enter-end="translate-x-0 opacity-100"
            x-transition:leave="transition ease-in duration-150"
            x-transition:leave-start="translate-x-0 opacity-100"
            x-transition:leave-end="translate-x-full opacity-90"
            class="absolute right-0 top-0 flex h-full w-full max-w-xl flex-col overflow-y-auto border-l border-zinc-200 bg-white shadow-2xl dark:border-zinc-700 dark:bg-zinc-950"
        >
            <header class="sticky top-0 z-10 border-b border-zinc-200 bg-white/95 p-5 backdrop-blur dark:border-zinc-800 dark:bg-zinc-950/95">
                <div class="flex items-start justify-between gap-4">
                    <div>
                        <p class="text-xs font-bold uppercase tracking-[0.18em] text-emerald-700 dark:text-lime-300">Filters</p>
                        <h2 class="mt-1 text-2xl font-semibold tracking-tight">{{ $title }}</h2>
                        @if($description)
                            <p class="mt-1 text-sm text-zinc-500">{{ $description }}</p>
                        @endif
                    </div>
                    <button
                        type="button"
                        x-on:click="filtersOpen = false"
                        class="rounded-full border border-zinc-200 p-2 text-zinc-500 transition hover:border-zinc-300 hover:text-zinc-950 dark:border-zinc-700 dark:hover:text-white"
                        aria-label="Close filters"
                    >
                        <svg class="size-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
                            <path stroke-linecap="round" stroke-linejoin="round" d="M6 6l12 12M18 6L6 18" />
                        </svg>
                    </button>
                </div>
            </header>

            <div class="flex-1 space-y-5 p-5">
                {{ $slot }}
            </div>
        </aside>
    </div>
</div>
