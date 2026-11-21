<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('listing_favourites', function (Blueprint $table) {
            $table->id();
            $table->foreignId('user_id')->constrained()->cascadeOnDelete();
            $table->string('external_id');
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
            $table->json('comps_snapshot')->nullable();
            $table->timestampTz('favourited_at')->index();
            $table->timestamps();

            $table->unique(['user_id', 'external_id']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('listing_favourites');
    }
};
