<div class="flex h-full flex-1 flex-col gap-6">
    <section class="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
        <div class="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div>
                <p class="text-xs uppercase tracking-[0.24em] text-zinc-500 dark:text-zinc-400">Admin</p>
                <h1 class="mt-2 text-2xl font-semibold text-zinc-950 dark:text-white">{{ $user->name }}</h1>
                <p class="mt-1 text-sm text-zinc-600 dark:text-zinc-300">{{ $user->email }}</p>
            </div>

            <a href="{{ route('users.index') }}" wire:navigate class="inline-flex items-center rounded-lg border border-zinc-300 px-3 py-2 text-sm font-medium text-zinc-700 transition hover:border-zinc-400 dark:border-zinc-700 dark:text-zinc-200 dark:hover:border-zinc-600">
                Back to users
            </a>
        </div>
    </section>

    <section class="grid gap-6 xl:grid-cols-3">
        <div class="space-y-6 xl:col-span-2">
            <div class="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
                <h2 class="text-lg font-semibold text-zinc-950 dark:text-white">Account Overview</h2>
                <div class="mt-4 grid gap-4 md:grid-cols-2">
                    <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-950">
                        <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Role</p>
                        <p class="mt-2 text-base font-semibold text-zinc-950 dark:text-white">{{ ucfirst($user->role) }}</p>
                    </div>
                    <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-950">
                        <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Email verification</p>
                        <p class="mt-2 text-base font-semibold text-zinc-950 dark:text-white">{{ $user->email_verified_at ? 'Verified' : 'Unverified' }}</p>
                    </div>
                    <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-950">
                        <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Created</p>
                        <p class="mt-2 text-base font-semibold text-zinc-950 dark:text-white">{{ $user->created_at?->format('d M Y H:i') }}</p>
                    </div>
                    <div class="rounded-xl border border-zinc-200 bg-zinc-50 p-4 dark:border-zinc-800 dark:bg-zinc-950">
                        <p class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Billing profile</p>
                        <p class="mt-2 text-base font-semibold text-zinc-950 dark:text-white">{{ $user->hasBillingProfile() ? 'Present' : 'Not set' }}</p>
                    </div>
                </div>
            </div>

            <div class="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
                <h2 class="text-lg font-semibold text-zinc-950 dark:text-white">Effective Permissions</h2>
                <div class="mt-4 flex flex-wrap gap-2">
                    @forelse ($permissions as $permission)
                        <span class="inline-flex rounded-full bg-zinc-100 px-3 py-1.5 text-sm font-medium text-zinc-700 dark:bg-zinc-800 dark:text-zinc-200">
                            {{ $permission }}
                        </span>
                    @empty
                        <p class="text-sm text-zinc-600 dark:text-zinc-300">No effective permissions assigned.</p>
                    @endforelse
                </div>
            </div>

            <div class="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
                <h2 class="text-lg font-semibold text-zinc-950 dark:text-white">Recent Subscriptions</h2>
                <div class="mt-4 overflow-x-auto">
                    <table class="min-w-full text-left text-sm">
                        <thead class="border-b border-zinc-200 dark:border-zinc-800">
                            <tr>
                                <th class="pb-3 font-medium text-zinc-600 dark:text-zinc-300">Type</th>
                                <th class="pb-3 font-medium text-zinc-600 dark:text-zinc-300">Status</th>
                                <th class="pb-3 font-medium text-zinc-600 dark:text-zinc-300">Price</th>
                                <th class="pb-3 font-medium text-zinc-600 dark:text-zinc-300">Ends</th>
                            </tr>
                        </thead>
                        <tbody class="divide-y divide-zinc-200 dark:divide-zinc-800">
                            @forelse ($recentSubscriptions as $subscription)
                                <tr>
                                    <td class="py-3 pr-4 text-zinc-950 dark:text-white">{{ $subscription->type }}</td>
                                    <td class="py-3 pr-4 text-zinc-600 dark:text-zinc-300">{{ str($subscription->stripe_status)->replace('_', ' ')->title() }}</td>
                                    <td class="py-3 pr-4 text-zinc-600 dark:text-zinc-300">{{ $subscription->stripe_price ?: 'N/A' }}</td>
                                    <td class="py-3 text-zinc-600 dark:text-zinc-300">{{ $subscription->ends_at?->format('d M Y H:i') ?: 'Active / ongoing' }}</td>
                                </tr>
                            @empty
                                <tr>
                                    <td colspan="4" class="py-6 text-sm text-zinc-500 dark:text-zinc-400">No subscriptions recorded in Cashier.</td>
                                </tr>
                            @endforelse
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="space-y-6">
            <div class="rounded-xl border border-zinc-200 bg-white p-5 shadow-sm dark:border-zinc-700 dark:bg-zinc-900">
                <h2 class="text-lg font-semibold text-zinc-950 dark:text-white">Billing Snapshot</h2>
                <dl class="mt-4 space-y-4 text-sm">
                    <div>
                        <dt class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Stripe customer</dt>
                        <dd class="mt-1 font-medium text-zinc-950 dark:text-white">{{ $user->stripe_id ?: 'Not created' }}</dd>
                    </div>
                    <div>
                        <dt class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Payment method</dt>
                        <dd class="mt-1 font-medium text-zinc-950 dark:text-white">
                            {{ $user->pm_type ? strtoupper($user->pm_type) : 'None' }}
                            @if ($user->pm_last_four)
                                •••• {{ $user->pm_last_four }}
                            @endif
                        </dd>
                    </div>
                    <div>
                        <dt class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Trial ends</dt>
                        <dd class="mt-1 font-medium text-zinc-950 dark:text-white">{{ $user->trial_ends_at?->format('d M Y H:i') ?: 'No trial' }}</dd>
                    </div>
                    <div>
                        <dt class="text-xs uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">Latest subscription</dt>
                        <dd class="mt-1 font-medium text-zinc-950 dark:text-white">{{ $latestSubscription ? str($latestSubscription->stripe_status)->replace('_', ' ')->title() : 'No subscription' }}</dd>
                    </div>
                </dl>
            </div>
        </div>
    </section>
</div>
