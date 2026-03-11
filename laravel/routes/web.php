<?php

use App\Models\User;
use Illuminate\Support\Facades\Route;

Route::view('/', 'welcome')->name('home');

Route::middleware(['auth', 'verified', 'admin'])->group(function () {
    Route::view('dashboard', 'dashboard')->name('dashboard');
    Route::view('auctions', 'auctions.index')->name('auctions.index');
    Route::view('users', 'users.index')->name('users.index');
    Route::get('users/{user}', function (User $user) {
        return view('users.show', ['user' => $user]);
    })->name('users.show');
});

require __DIR__.'/settings.php';
