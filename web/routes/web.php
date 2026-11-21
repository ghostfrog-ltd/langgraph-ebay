<?php

use App\Http\Controllers\GoogleAuthController;
use App\Http\Controllers\ListingFavouriteController;
use App\Livewire\Calculator\Index as CalculatorIndex;
use App\Livewire\Favourites\Index as FavouriteIndex;
use App\Livewire\Favourites\Show as FavouriteShow;
use App\Livewire\Links\ThreeDPrintLeaderboard;
use App\Livewire\Listings\Index;
use App\Livewire\Listings\Show;
use App\Livewire\Listings\SourceIndex;
use App\Livewire\ModelKeyComps\Show as ModelKeyCompShow;
use App\Livewire\Search\Index as SearchIndex;
use Illuminate\Support\Facades\Route;

Route::redirect('/', '/dashboard')->name('home');

Route::middleware('guest')->group(function () {
    Route::get('auth/google', [GoogleAuthController::class, 'redirect'])->name('auth.google.redirect');
    Route::get('auth/google/callback', [GoogleAuthController::class, 'callback'])->name('auth.google.callback');
});

Route::middleware(['auth', 'verified'])->group(function () {
    Route::livewire('dashboard', Index::class)->name('dashboard');
    Route::livewire('calculator', CalculatorIndex::class)->name('calculator.index');
    Route::livewire('search', SearchIndex::class)->name('search.index');
    Route::livewire('favourites', FavouriteIndex::class)->name('favourites.index');
    Route::livewire('links/3d-print-leaderboard', ThreeDPrintLeaderboard::class)->name('links.3d-print-leaderboard');
    Route::post('listings/{listing}/favourite', [ListingFavouriteController::class, 'store'])->name('listings.favourites.store');
    Route::delete('favourites/{favourite}', [ListingFavouriteController::class, 'destroy'])->name('favourites.destroy');
    Route::livewire('favourites/{favourite}', FavouriteShow::class)->name('favourites.show');
    Route::livewire('model-key-comps/{modelKeyComp}', ModelKeyCompShow::class)->name('model-key-comps.show');
    Route::livewire('sources/{source}', SourceIndex::class)->name('sources.show');
    Route::livewire('listings/{listing}', Show::class)->name('listings.show');
});

require __DIR__.'/settings.php';
