<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        foreach (['published_comps', 'published_sold_evidence'] as $tableName) {
            Schema::table($tableName, function (Blueprint $table): void {
                $table->string('visual_condition_grade', 16)->nullable()->after('condition_grade');
                $table->boolean('visual_condition_checked')->default(false)->after('visual_condition_grade')->index();
                $table->boolean('visual_review_required')->default(false)->after('visual_condition_checked');
                $table->json('visual_damage_flags')->nullable()->after('visual_review_required');
                $table->string('visual_model_name')->nullable()->after('visual_damage_flags');
                $table->timestampTz('visual_checked_at')->nullable()->after('visual_model_name');
            });
        }

        Schema::table('published_model_key_comp_rows', function (Blueprint $table): void {
            $table->string('visual_condition_grade', 16)->nullable()->after('bids_count');
            $table->boolean('visual_condition_checked')->default(false)->after('visual_condition_grade')->index();
            $table->boolean('visual_review_required')->default(false)->after('visual_condition_checked');
            $table->json('visual_damage_flags')->nullable()->after('visual_review_required');
            $table->string('visual_model_name')->nullable()->after('visual_damage_flags');
            $table->timestampTz('visual_checked_at')->nullable()->after('visual_model_name');
        });

        Schema::table('listing_favourites', function (Blueprint $table): void {
            $table->string('text_condition_grade', 4)->nullable()->after('condition_grade');
            $table->string('visual_condition_grade', 16)->nullable()->after('text_condition_grade');
            $table->boolean('visual_condition_checked')->default(false)->after('visual_condition_grade')->index();
            $table->boolean('visual_review_required')->default(false)->after('visual_condition_checked');
            $table->json('visual_damage_flags')->nullable()->after('visual_review_required');
            $table->string('visual_model_name')->nullable()->after('visual_damage_flags');
            $table->timestampTz('visual_checked_at')->nullable()->after('visual_model_name');
        });
    }

    public function down(): void
    {
        Schema::table('listing_favourites', function (Blueprint $table): void {
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

        foreach (['published_comps', 'published_sold_evidence', 'published_model_key_comp_rows'] as $tableName) {
            Schema::table($tableName, function (Blueprint $table): void {
                $table->dropColumn([
                    'visual_condition_grade',
                    'visual_condition_checked',
                    'visual_review_required',
                    'visual_damage_flags',
                    'visual_model_name',
                    'visual_checked_at',
                ]);
            });
        }
    }
};
