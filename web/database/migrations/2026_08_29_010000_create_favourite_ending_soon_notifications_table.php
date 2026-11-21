<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('favourite_ending_soon_notifications', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('user_id')->constrained()->cascadeOnDelete();
            $table->foreignId('listing_favourite_id')->constrained()->cascadeOnDelete();
            $table->string('external_id')->index();
            $table->timestampTz('listing_end_time')->index();
            $table->timestampTz('notification_window_start');
            $table->timestampTz('sent_at')->index();
            $table->timestamps();

            $table->unique(['listing_favourite_id', 'notification_window_start'], 'fav_ending_soon_once_per_hour');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('favourite_ending_soon_notifications');
    }
};
