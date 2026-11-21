<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('three_d_print_sellers', function (Blueprint $table) {
            $table->id();
            $table->unsignedInteger('rank')->index();
            $table->string('marketplace', 32)->default('EBAY_GB');
            $table->string('seller_username')->unique();
            $table->unsignedInteger('item_count');
            $table->decimal('active_value', 12, 2);
            $table->decimal('max_price', 12, 2);
            $table->unsignedInteger('feedback_score')->default(0);
            $table->decimal('feedback_percentage', 5, 2)->default(0);
            $table->json('matched_queries');
            $table->json('example_titles');
            $table->text('example_url')->nullable();
            $table->uuid('sync_batch_id')->index();
            $table->timestampTz('snapshot_at')->index();
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('three_d_print_sellers');
    }
};
