<?php

namespace App\Http\Controllers;

use App\Models\User;
use Illuminate\Http\RedirectResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Str;
use Laravel\Socialite\Facades\Socialite;
use Symfony\Component\HttpFoundation\RedirectResponse as SymfonyRedirectResponse;
use Throwable;

class GoogleAuthController extends Controller
{
    public function redirect(): RedirectResponse|SymfonyRedirectResponse
    {
        if (! $this->isConfigured()) {
            return redirect()
                ->route('login')
                ->with('status', 'Google login is not configured yet.');
        }

        return Socialite::driver('google')->redirect();
    }

    public function callback(Request $request): RedirectResponse
    {
        if (! $this->isConfigured()) {
            return redirect()
                ->route('login')
                ->with('status', 'Google login is not configured yet.');
        }

        try {
            $googleUser = Socialite::driver('google')->user();
        } catch (Throwable) {
            return redirect()
                ->route('login')
                ->withErrors(['email' => 'Google sign-in failed. Please try again.']);
        }

        $email = $googleUser->getEmail();

        if (! is_string($email) || $email === '') {
            return redirect()
                ->route('login')
                ->withErrors(['email' => 'Google did not return an email address.']);
        }

        $rawGoogleId = $googleUser->getId();
        $googleId = $rawGoogleId !== '' ? $rawGoogleId : null;

        $userQuery = User::query()->where('email', $email);

        if ($googleId !== null) {
            $userQuery->orWhere('google_id', $googleId);
        }

        $user = $userQuery->first();

        if ($user === null) {
            $user = User::query()->create([
                'name' => $googleUser->getName() ?: Str::before($email, '@'),
                'email' => $email,
                'email_verified_at' => now(),
                'password' => Str::password(32),
                'google_id' => $googleId,
                'google_avatar' => $googleUser->getAvatar(),
            ]);
        } else {
            $user->forceFill([
                'google_id' => $googleId,
                'google_avatar' => $googleUser->getAvatar(),
                'email_verified_at' => $user->email_verified_at ?? now(),
            ])->save();
        }

        Auth::login($user);
        $request->session()->regenerate();

        return redirect()->intended(route('dashboard', absolute: false));
    }

    private function isConfigured(): bool
    {
        return filled(config('services.google.client_id'))
            && filled(config('services.google.client_secret'));
    }
}
