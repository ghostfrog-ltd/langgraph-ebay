<!DOCTYPE html>
<html lang="{{ str_replace('_', '-', app()->getLocale()) }}">
    <head>
        @include('partials.head')
    </head>
    <body class="min-h-screen bg-white dark:bg-zinc-800">
        <flux:sidebar sticky collapsible="mobile" class="border-e border-zinc-200 bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-900">
            <flux:sidebar.header>
                <x-app-logo :sidebar="true" href="{{ route('dashboard') }}" wire:navigate />
                <flux:sidebar.collapse class="lg:hidden" />
            </flux:sidebar.header>

            @php
                $sidebarSourceCounts = \App\Support\SourceCatalog::visibleSourceCounts();
                $sidebarFavouriteCount = auth()->user()->listingFavourites()->count();
            @endphp

            <flux:sidebar.nav>
                <flux:sidebar.group :heading="__('Buy desk')" class="grid">
                    <flux:sidebar.item icon="magnifying-glass" :href="route('dashboard')" :current="request()->routeIs('dashboard')" wire:navigate>
                        {{ __('Live listings') }}
                    </flux:sidebar.item>
                    <flux:sidebar.item icon="document-magnifying-glass" :href="route('search.index')" :current="request()->routeIs('search.*')" wire:navigate>
                        {{ __('Search') }}
                    </flux:sidebar.item>
                    <flux:sidebar.item icon="calculator" :href="route('calculator.index')" :current="request()->routeIs('calculator.*')" wire:navigate>
                        {{ __('Calculator') }}
                    </flux:sidebar.item>
                </flux:sidebar.group>

                <flux:sidebar.group :heading="__('Favourites')" class="grid">
                    <flux:sidebar.item icon="heart" :href="route('favourites.index')" :current="request()->routeIs('favourites.*')" wire:navigate>
                        {{ __('Saved listings') }}
                        <span class="text-zinc-400 dark:text-zinc-500">({{ number_format($sidebarFavouriteCount) }})</span>
                    </flux:sidebar.item>
                </flux:sidebar.group>

                @if($sidebarSourceCounts->isNotEmpty())
                    <flux:sidebar.group :heading="__('Sources')" class="grid">
                        @foreach($sidebarSourceCounts as $sidebarSource => $sidebarSourceCount)
                            <flux:sidebar.item icon="tag" :href="route('sources.show', $sidebarSource)" :current="request()->routeIs('sources.show') && request()->route('source') === $sidebarSource" wire:navigate>
                                {{ \App\Support\SourceCatalog::label($sidebarSource) }}
                                <span class="text-zinc-400 dark:text-zinc-500">({{ number_format($sidebarSourceCount) }})</span>
                            </flux:sidebar.item>
                        @endforeach
                    </flux:sidebar.group>
                @endif
            </flux:sidebar.nav>

            <flux:spacer />

            <x-desktop-user-menu class="hidden lg:block" :name="auth()->user()->name" />
        </flux:sidebar>

        <!-- Mobile User Menu -->
        <flux:header class="lg:hidden">
            <flux:sidebar.toggle class="lg:hidden" icon="bars-2" inset="left" />

            <flux:spacer />

            <flux:dropdown position="top" align="end">
                <flux:profile
                    :initials="auth()->user()->initials()"
                    icon-trailing="chevron-down"
                />

                <flux:menu>
                    <flux:menu.radio.group>
                        <div class="p-0 text-sm font-normal">
                            <div class="flex items-center gap-2 px-1 py-1.5 text-start text-sm">
                                <flux:avatar
                                    :name="auth()->user()->name"
                                    :initials="auth()->user()->initials()"
                                />

                                <div class="grid flex-1 text-start text-sm leading-tight">
                                    <flux:heading class="truncate">{{ auth()->user()->name }}</flux:heading>
                                    <flux:text class="truncate">{{ auth()->user()->email }}</flux:text>
                                </div>
                            </div>
                        </div>
                    </flux:menu.radio.group>

                    <flux:menu.separator />

                    <flux:menu.radio.group>
                        <flux:menu.item :href="route('profile.edit')" icon="cog" wire:navigate>
                            {{ __('Settings') }}
                        </flux:menu.item>
                    </flux:menu.radio.group>

                    <flux:menu.separator />

                    <form method="POST" action="{{ route('logout') }}" class="w-full">
                        @csrf
                        <flux:menu.item
                            as="button"
                            type="submit"
                            icon="arrow-right-start-on-rectangle"
                            class="w-full cursor-pointer"
                            data-test="logout-button"
                        >
                            {{ __('Log out') }}
                        </flux:menu.item>
                    </form>
                </flux:menu>
            </flux:dropdown>
        </flux:header>

        {{ $slot }}

        @persist('toast')
            <flux:toast.group>
                <flux:toast />
            </flux:toast.group>
        @endpersist

        @fluxScripts
    </body>
</html>
