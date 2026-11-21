<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('published_listings', function (Blueprint $table): void {
            $table->string('text_condition_grade', 4)->nullable()->after('condition_grade');
            $table->string('visual_condition_grade', 16)->nullable()->after('text_condition_grade');
            $table->boolean('visual_condition_checked')->default(false)->after('visual_condition_grade');
            $table->boolean('visual_review_required')->default(false)->after('visual_condition_checked');
            $table->json('visual_damage_flags')->nullable()->after('visual_review_required');
            $table->string('visual_model_name')->nullable()->after('visual_damage_flags');
            $table->timestampTz('visual_checked_at')->nullable()->after('visual_model_name');
        });
    }

    public function down(): void
    {
        Schema::table('published_listings', function (Blueprint $table): void {
            $table->dropColumn([
                'text_condition_grade',
                'visual_condition_grade',
                'visual_condition_checked',
                'visual_review_required',
                'visual_damage_flags',
                'visual_model_name',
                'visual_checked_at',
            ]);
        });
    }
};
