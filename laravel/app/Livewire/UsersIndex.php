<?php

namespace App\Livewire;

use App\Models\User;
use Illuminate\Contracts\View\View;
use Livewire\Component;
use Livewire\WithPagination;

class UsersIndex extends Component
{
    use WithPagination;

    public int $perPage = 25;

    public function render(): View
    {
        return view('livewire.users-index', [
            'users' => User::query()
                ->with('latestSubscription')
                ->orderBy('name')
                ->paginate($this->perPage),
        ]);
    }
}
