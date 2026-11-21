<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('sync_runs', function (Blueprint $table) {
            $table->id();
            $table->uuid('batch_id')->unique();
            $table->timestampTz('generated_at');
            $table->timestampTz('received_at');
            $table->unsignedInteger('listing_count');
            $table->unsignedInteger('comp_count');
            $table->string('payload_sha256', 64);
            $table->json('summary')->nullable();
            $table->timestamps();
        });

        Schema::create('published_listings', function (Blueprint $table) {
            $table->string('external_id')->primary();
            $table->string('source')->index();
            $table->text('title');
            $table->text('url');
            $table->string('model_key')->index();
            $table->string('condition_grade', 4);
            $table->string('sale_type')->nullable();
            $table->timestampTz('end_time')->nullable()->index();
            $table->decimal('current_price', 12, 2);
            $table->decimal('sold_median', 12, 2);
            $table->unsignedInteger('comp_samples');
            $table->decimal('max_safe_buy', 12, 2);
            $table->decimal('expected_profit', 12, 2)->index();
            $table->decimal('expected_roi', 10, 4);
            $table->decimal('price_gap_to_max_buy', 12, 2);
            $table->unsignedInteger('effective_min_comps');
            $table->text('signal');
            $table->uuid('sync_batch_id')->index();
            $table->timestampTz('published_at');
            $table->timestamps();
        });

        Schema::create('published_comps', function (Blueprint $table) {
            $table->id();
            $table->string('listing_external_id');
            $table->string('comp_external_id');
            $table->string('source')->nullable();
            $table->text('title');
            $table->text('url')->nullable();
            $table->string('sale_type')->nullable();
            $table->timestampTz('end_time')->nullable();
            $table->decimal('final_price', 12, 2);
            $table->unsignedInteger('bids_count')->nullable();
            $table->string('condition_grade', 4)->nullable();
            $table->string('model_key')->nullable();
            $table->timestamps();

            $table->foreign('listing_external_id')
                ->references('external_id')
                ->on('published_listings')
                ->cascadeOnDelete();
            $table->unique(['listing_external_id', 'comp_external_id']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('published_comps');
        Schema::dropIfExists('published_listings');
        Schema::dropIfExists('sync_runs');
    }
};
