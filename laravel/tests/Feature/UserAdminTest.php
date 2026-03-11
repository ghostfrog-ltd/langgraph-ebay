<?php

use App\Livewire\UserDetail;
use App\Livewire\UsersIndex;
use App\Models\User;
use Illuminate\Support\Facades\DB;
use Livewire\Livewire;

test('admin can view users index with roles and subscription state', function () {
    $admin = User::factory()->create([
        'role' => 'admin',
    ]);

    $subscribedUser = User::factory()->create([
        'name' => 'Subscribed User',
        'email' => 'subscribed@example.com',
        'role' => 'member',
        'stripe_id' => 'cus_12345',
        'pm_type' => 'card',
        'pm_last_four' => '4242',
    ]);

    DB::table('subscriptions')->insert([
        'user_id' => $subscribedUser->id,
        'type' => 'default',
        'stripe_id' => 'sub_12345',
        'stripe_status' => 'active',
        'stripe_price' => 'price_pro',
        'quantity' => 1,
        'trial_ends_at' => null,
        'ends_at' => null,
        'created_at' => now(),
        'updated_at' => now(),
    ]);

    $this->actingAs($admin);

    $this->get(route('users.index'))->assertOk();

    Livewire::test(UsersIndex::class)
        ->assertSee('Subscribed User')
        ->assertSee('Customer on file')
        ->assertSee('Active')
        ->assertSee('price_pro')
        ->assertSee('8 granted');
});

test('admin can view user detail with permissions and recent subscriptions', function () {
    $admin = User::factory()->create([
        'role' => 'admin',
    ]);

    $user = User::factory()->create([
        'name' => 'Detail User',
        'email' => 'detail@example.com',
        'role' => 'member',
        'stripe_id' => 'cus_67890',
        'pm_type' => 'card',
        'pm_last_four' => '1111',
        'trial_ends_at' => now()->addWeek(),
    ]);

    DB::table('subscriptions')->insert([
        'user_id' => $user->id,
        'type' => 'default',
        'stripe_id' => 'sub_67890',
        'stripe_status' => 'trialing',
        'stripe_price' => 'price_basic',
        'quantity' => 1,
        'trial_ends_at' => now()->addWeek(),
        'ends_at' => null,
        'created_at' => now(),
        'updated_at' => now(),
    ]);

    $this->actingAs($admin);

    $this->get(route('users.show', $user))->assertOk();

    Livewire::test(UserDetail::class, ['user' => $user])
        ->assertSee('Detail User')
        ->assertSee('dashboard.view')
        ->assertSee('price_basic')
        ->assertSee('Trialing')
        ->assertSee('cus_67890');
});
