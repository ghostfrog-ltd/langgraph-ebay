<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('ebay_listings', function (Blueprint $table) {
            $table->string('source_platform')->nullable()->after('ebay_id');
            $table->string('category_name')->nullable()->after('source_platform');
            $table->json('attributes_json')->nullable()->after('llm_analysis');
        });
    }

    public function down(): void
    {
        Schema::table('ebay_listings', function (Blueprint $table) {
            $table->dropColumn(['source_platform', 'category_name', 'attributes_json']);
        });
    }
};
