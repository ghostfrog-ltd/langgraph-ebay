<?php

use App\Models\User;

test('registration screen is not available', function () {
    $response = $this->get('/register');

    $response->assertNotFound();
});

test('new users can not self-register', function () {
    $response = $this->post('/register', [
        'name' => 'John Doe',
        'email' => 'test@example.com',
        'password' => 'password',
        'password_confirmation' => 'password',
    ]);

    $response->assertNotFound();

    expect(User::query()->where('email', 'test@example.com')->exists())->toBeFalse();
    $this->assertGuest();
});
