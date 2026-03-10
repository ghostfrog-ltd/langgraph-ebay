<?php

namespace Database\Seeders;

use App\Models\User;
use Illuminate\Database\Seeder;

class AdminUserSeeder extends Seeder
{
    public function run(): void
    {
        $email = env('ADMIN_EMAIL');
        $password = env('ADMIN_PASSWORD');
        $name = env('ADMIN_NAME', 'GhostFrog Admin');

        if (! $email || ! $password) {
            $this->command?->warn('Skipping AdminUserSeeder because ADMIN_EMAIL or ADMIN_PASSWORD is missing.');

            return;
        }

        User::updateOrCreate(
            ['email' => $email],
            [
                'name' => $name,
                'role' => 'admin',
                'password' => bcrypt($password),
                'email_verified_at' => now(),
            ]
        );
    }
}
