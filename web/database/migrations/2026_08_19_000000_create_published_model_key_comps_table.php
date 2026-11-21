<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('published_model_key_comps', function (Blueprint $table) {
            $table->id();
            $table->string('source')->index();
            $table->string('model_key')->index();
            $table->string('condition_grade', 4);
            $table->unsignedInteger('samples');
            $table->decimal('median_final_price', 12, 2);
            $table->timestampTz('computed_at')->nullable();
            $table->timestampTz('synced_at');
            $table->timestamps();

            $table->unique(['model_key', 'condition_grade']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('published_model_key_comps');
    }
};
