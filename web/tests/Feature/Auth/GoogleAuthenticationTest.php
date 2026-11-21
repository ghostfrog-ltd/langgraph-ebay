<?php

namespace Tests\Feature\Auth;

use App\Models\User;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Laravel\Fortify\Features;
use Laravel\Socialite\Contracts\User as SocialiteUserContract;
use Laravel\Socialite\Facades\Socialite;
use Mockery;
use Tests\TestCase;

class GoogleAuthenticationTest extends TestCase
{
    use RefreshDatabase;

    public function test_login_screen_shows_google_login(): void
    {
        $response = $this->get(route('login'));

        $response
            ->assertOk()
            ->assertSee('Continue with Google');
    }

    public function test_register_screen_shows_google_login(): void
    {
        $this->skipUnlessFortifyHas(Features::registration());

        $response = $this->get(route('register'));

        $response
            ->assertOk()
            ->assertSee('Continue with Google');
    }

    public function test_google_redirect_requires_configuration(): void
    {
        config([
            'services.google.client_id' => null,
            'services.google.client_secret' => null,
        ]);

        $response = $this->get(route('auth.google.redirect'));

        $response
            ->assertRedirect(route('login', absolute: false))
            ->assertSessionHas('status', 'Google login is not configured yet.');
    }

    public function test_google_redirect_sends_user_to_google(): void
    {
        $this->configureGoogle();

        $provider = Mockery::mock();
        $provider
            ->shouldReceive('redirect')
            ->once()
            ->andReturn(redirect()->away('https://accounts.google.com/o/oauth2/auth'));

        Socialite::shouldReceive('driver')
            ->once()
            ->with('google')
            ->andReturn($provider);

        $response = $this->get(route('auth.google.redirect'));

        $response->assertRedirect('https://accounts.google.com/o/oauth2/auth');
    }

    public function test_google_callback_creates_and_authenticates_user(): void
    {
        $this->configureGoogle();
        $this->mockGoogleUser(
            googleId: 'google-123',
            email: 'buyer@example.com',
            name: 'Evidence Buyer',
            avatar: 'https://example.com/avatar.png',
        );

        $response = $this->get(route('auth.google.callback'));

        $response->assertRedirect(route('dashboard', absolute: false));
        $this->assertAuthenticated();
        $this->assertDatabaseHas('users', [
            'email' => 'buyer@example.com',
            'name' => 'Evidence Buyer',
            'google_id' => 'google-123',
            'google_avatar' => 'https://example.com/avatar.png',
        ]);
    }

    public function test_google_callback_links_existing_email_account(): void
    {
        $this->configureGoogle();
        $user = User::factory()->unverified()->create([
            'email' => 'existing@example.com',
            'google_id' => null,
        ]);

        $this->mockGoogleUser(
            googleId: 'google-existing',
            email: 'existing@example.com',
            name: 'Existing User',
            avatar: null,
        );

        $response = $this->get(route('auth.google.callback'));

        $response->assertRedirect(route('dashboard', absolute: false));
        $this->assertAuthenticatedAs($user->fresh());
        $this->assertDatabaseHas('users', [
            'id' => $user->id,
            'email' => 'existing@example.com',
            'google_id' => 'google-existing',
        ]);
        $this->assertNotNull($user->fresh()->email_verified_at);
    }

    private function configureGoogle(): void
    {
        config([
            'services.google.client_id' => 'client-id',
            'services.google.client_secret' => 'client-secret',
        ]);
    }

    private function mockGoogleUser(string $googleId, string $email, string $name, ?string $avatar): void
    {
        $googleUser = Mockery::mock(SocialiteUserContract::class);
        $googleUser->shouldReceive('getId')->andReturn($googleId);
        $googleUser->shouldReceive('getEmail')->andReturn($email);
        $googleUser->shouldReceive('getName')->andReturn($name);
        $googleUser->shouldReceive('getAvatar')->andReturn($avatar);

        $provider = Mockery::mock();
        $provider
            ->shouldReceive('user')
            ->once()
            ->andReturn($googleUser);

        Socialite::shouldReceive('driver')
            ->once()
            ->with('google')
            ->andReturn($provider);
    }
}
