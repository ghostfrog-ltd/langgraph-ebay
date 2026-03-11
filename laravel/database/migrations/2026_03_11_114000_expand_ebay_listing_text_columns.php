<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\DB;

return new class extends Migration
{
    public function up(): void
    {
        if (Schema::getConnection()->getDriverName() === 'sqlite') {
            return;
        }

        DB::statement('ALTER TABLE ebay_listings ALTER COLUMN title TYPE TEXT');
        DB::statement('ALTER TABLE ebay_listings ALTER COLUMN image_url TYPE TEXT');
        DB::statement('ALTER TABLE ebay_listings ALTER COLUMN item_url TYPE TEXT');
    }

    public function down(): void
    {
        if (Schema::getConnection()->getDriverName() === 'sqlite') {
            return;
        }

        DB::statement('ALTER TABLE ebay_listings ALTER COLUMN title TYPE VARCHAR(255)');
        DB::statement('ALTER TABLE ebay_listings ALTER COLUMN image_url TYPE VARCHAR(255)');
        DB::statement('ALTER TABLE ebay_listings ALTER COLUMN item_url TYPE VARCHAR(255)');
    }
};
