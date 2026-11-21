<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('published_sold_evidence', function (Blueprint $table) {
            $table->string('external_id')->primary();
            $table->string('source')->index();
            $table->text('title');
            $table->text('url')->nullable();
            $table->string('sale_type')->nullable();
            $table->timestampTz('end_time')->nullable()->index();
            $table->decimal('final_price', 12, 2)->index();
            $table->unsignedInteger('bids_count')->nullable();
            $table->string('condition_grade', 4)->nullable()->index();
            $table->string('model_key')->nullable()->index();
            $table->timestampTz('synced_at')->index();
            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('published_sold_evidence');
    }
};
