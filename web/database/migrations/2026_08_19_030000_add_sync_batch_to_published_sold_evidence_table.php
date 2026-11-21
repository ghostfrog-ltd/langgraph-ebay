<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('published_sold_evidence', function (Blueprint $table) {
            $table->uuid('sync_batch_id')->nullable()->index()->after('synced_at');
        });
    }

    public function down(): void
    {
        Schema::table('published_sold_evidence', function (Blueprint $table) {
            $table->dropColumn('sync_batch_id');
        });
    }
};
