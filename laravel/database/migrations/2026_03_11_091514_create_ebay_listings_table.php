<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('ebay_listings', function (Blueprint $table) {
            $table->id();
            $table->string('ebay_id')->unique();
            $table->string('title');
            $table->string('image_url')->nullable();
            $table->decimal('current_price', 10, 2);
            $table->decimal('estimated_market_value', 10, 2);
            $table->string('item_url');
            $table->text('llm_analysis')->nullable();
            $table->enum('pipeline_state', ['ingested', 'analyzing', 'opportunity', 'dismissed'])
                ->default('ingested');
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('ebay_listings');
    }
};
