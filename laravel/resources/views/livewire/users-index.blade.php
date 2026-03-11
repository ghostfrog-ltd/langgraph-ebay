<div class="flex h-full flex-1 flex-col gap-6">
    <section class="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div>
                <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Admin</p>
                <h1 class="mt-2 text-2xl font-semibold text-zinc-950 dark:text-white">Users</h1>
                <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">Roles, effective permissions, and Cashier subscription state.</p>
            </div>

            <label class="flex items-center gap-2">
                <span class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Per page</span>
                <select wire:model.live="perPage" class="rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-zinc-950 outline-none transition focus:border-zinc-500 dark:border-zinc-700 dark:bg-zinc-950 dark:text-white">
                    <option value="10">10</option>
                    <option value="25">25</option>
                    <option value="50">50</option>
                </select>
            </label>
        </div>
    </section>

    <section class="overflow-hidden rounded-xl border border-zinc-200 bg-white shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="overflow-x-auto">
            <table class="min-w-full text-left text-sm">
                <thead class="border-b border-zinc-200 bg-zinc-50 dark:border-zinc-800 dark:bg-zinc-950/60">
                    <tr>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">User</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Role</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Permissions</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Billing</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Subscription</th>
                        <th class="px-4 py-3 font-medium text-zinc-600 dark:text-zinc-300">Action</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-zinc-200 dark:divide-zinc-800">
                    @foreach ($users as $user)
                        @php
                            $subscription = $user->latestSubscription;
                            $roleClasses = $user->isAdmin()
                                ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-950/50 dark:text-emerald-300'
                                : 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-200';
                            $subscriptionClasses = $subscription
                                ? 'bg-sky-100 text-sky-700 dark:bg-sky-950/50 dark:text-sky-300'
                                : 'bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300';
                        @endphp
                        <tr wire:key="user-row-{{ $user->id }}" class="transition hover:bg-zinc-50 dark:hover:bg-zinc-950/60">
                            <td class="px-4 py-3">
                                <div>
                                    <p class="font-medium text-zinc-950 dark:text-white">{{ $user->name }}</p>
                                    <p class="mt-1 text-xs text-zinc-500 dark:text-zinc-400">{{ $user->email }}</p>
                                </div>
                            </td>
                            <td class="px-4 py-3">
                                <span class="inline-flex rounded-full px-2.5 py-1 text-xs font-medium {{ $roleClasses }}">
                                    {{ ucfirst($user->role) }}
                                </span>
                            </td>
                            <td class="px-4 py-3 text-zinc-600 dark:text-zinc-300">
                                <div class="font-medium text-zinc-950 dark:text-white">{{ count($user->permissionLabels()) }} granted</div>
                                <div class="mt-1 text-xs text-zinc-500 dark:text-zinc-400">{{ implode(', ', array_slice($user->permissionLabels(), 0, 2)) ?: 'No permissions' }}</div>
                            </td>
                            <td class="px-4 py-3 text-zinc-600 dark:text-zinc-300">
                                @if ($user->hasBillingProfile())
                                    <div class="font-medium text-zinc-950 dark:text-white">Customer on file</div>
                                    <div class="mt-1 text-xs text-zinc-500 dark:text-zinc-400">{{ strtoupper($user->pm_type ?? 'N/A') }} {{ $user->pm_last_four ? '•••• '.$user->pm_last_four : '' }}</div>
                                @else
                                    <div class="font-medium">No billing profile</div>
                                @endif
                            </td>
                            <td class="px-4 py-3">
                                <span class="inline-flex rounded-full px-2.5 py-1 text-xs font-medium {{ $subscriptionClasses }}">
                                    {{ $subscription ? str($subscription->stripe_status)->replace('_', ' ')->title() : 'None' }}
                                </span>
                                @if ($subscription?->stripe_price)
                                    <div class="mt-1 text-xs text-zinc-500 dark:text-zinc-400">{{ $subscription->stripe_price }}</div>
                                @endif
                            </td>
                            <td class="px-4 py-3">
                                <a href="{{ route('users.show', $user) }}" wire:navigate class="inline-flex items-center rounded-lg border border-zinc-300 px-3 py-2 text-sm font-medium text-zinc-700 transition hover:border-zinc-400 dark:border-zinc-700 dark:text-zinc-200 dark:hover:border-zinc-600">
                                    View details
                                </a>
                            </td>
                        </tr>
                    @endforeach
                </tbody>
            </table>
        </div>

        <div class="border-t border-zinc-200 px-4 py-3 dark:border-zinc-800">
            {{ $users->links() }}
        </div>
    </section>
</div>
