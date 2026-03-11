<div wire:poll.10s class="space-y-3 px-3 pb-3">
    <div class="rounded-xl border border-zinc-200 bg-white/80 p-3 shadow-sm dark:border-zinc-700 dark:bg-zinc-950/70">
        <div class="flex items-center justify-between gap-3">
            <div>
                <p class="text-[11px] uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Pipeline</p>
                <h2 class="mt-1 text-sm font-semibold text-zinc-950 dark:text-white">Status Monitor</h2>
            </div>

            <button
                type="button"
                wire:click="purgeDismissed"
                class="rounded-md border border-zinc-300 px-2.5 py-1 text-[11px] font-medium text-zinc-600 transition hover:border-zinc-400 hover:text-zinc-950 dark:border-zinc-700 dark:text-zinc-300 dark:hover:border-zinc-500 dark:hover:text-white"
            >
                Purge Dismissed
            </button>
        </div>

        <div class="mt-4 space-y-2">
            @foreach ($states as $state => $label)
                <div class="flex items-center justify-between rounded-lg bg-zinc-100/80 px-2.5 py-2 text-sm dark:bg-zinc-900">
                    <span class="text-zinc-600 dark:text-zinc-300">{{ $label }}</span>
                    <span class="font-semibold text-zinc-950 dark:text-white">{{ (int) ($counts[$state] ?? 0) }}</span>
                </div>
            @endforeach
        </div>

        <div class="mt-4 rounded-lg border border-dashed border-zinc-200 px-2.5 py-2 text-xs text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
            <span class="font-medium text-zinc-700 dark:text-zinc-200">Last Synced:</span>
            @if ($lastSyncedAt)
                {{ \Illuminate\Support\Carbon::parse($lastSyncedAt)->setTimezone(config('app.timezone'))->format('d M Y H:i:s') }}
            @else
                Waiting for engine
            @endif
        </div>
    </div>
</div>
