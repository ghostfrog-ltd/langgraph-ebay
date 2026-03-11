<?php

namespace App\Livewire;

use App\Models\User;
use Illuminate\Contracts\View\View;
use Livewire\Component;

class UserDetail extends Component
{
    public User $user;

    public function mount(User $user): void
    {
        $this->user = $user->load(['latestSubscription', 'subscriptions' => fn ($query) => $query->latest()->limit(5)]);
    }

    public function render(): View
    {
        return view('livewire.user-detail', [
            'permissions' => $this->user->permissionLabels(),
            'latestSubscription' => $this->user->latestSubscription,
            'recentSubscriptions' => $this->user->subscriptions,
        ]);
    }
}
