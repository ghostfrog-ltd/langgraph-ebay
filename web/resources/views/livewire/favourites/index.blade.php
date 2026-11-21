<div class="mx-auto flex w-full max-w-7xl flex-1 flex-col gap-7">
    <section class="rounded-2xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="flex flex-col justify-between gap-3 sm:flex-row sm:items-end">
            <div>
                <p class="text-xs font-bold uppercase tracking-[0.18em] text-rose-700 dark:text-rose-400">Your watch shelf</p>
                <h1 class="text-3xl font-semibold tracking-tight">Favourites</h1>
            </div>
            <p class="max-w-lg text-sm text-zinc-500">
                {{ number_format($favouriteCount) }} saved listings. These are user-specific and stay here even after the auction ends or the live listing drops out of sync.
            </p>
        </div>
    </section>

    <section>
        <div class="grid gap-4">
            @forelse($favourites as $favourite)
                <x-favourite-card :favourite="$favourite" />
            @empty
                <div class="rounded-2xl border border-dashed border-zinc-300 p-10 text-center dark:border-zinc-700">
                    <p class="text-2xl font-semibold">No favourites yet.</p>
                    <p class="mt-2 text-sm text-zinc-500">Use “Add favourite” on any listing to save it here.</p>
                </div>
            @endforelse
        </div>
        <div class="mt-5">{{ $favourites->links() }}</div>
    </section>
</div>
