<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('published_model_key_comp_rows', function (Blueprint $table) {
            $table->id();
            $table->foreignId('published_model_key_comp_id')
                ->constrained()
                ->cascadeOnDelete();
            $table->string('comp_external_id');
            $table->string('source')->nullable();
            $table->text('title');
            $table->text('url')->nullable();
            $table->string('sale_type')->nullable();
            $table->timestampTz('end_time')->nullable()->index();
            $table->decimal('final_price', 12, 2);
            $table->unsignedInteger('bids_count')->nullable();
            $table->timestamps();

            $table->unique(['published_model_key_comp_id', 'comp_external_id'], 'published_model_key_comp_rows_unique');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('published_model_key_comp_rows');
    }
};
