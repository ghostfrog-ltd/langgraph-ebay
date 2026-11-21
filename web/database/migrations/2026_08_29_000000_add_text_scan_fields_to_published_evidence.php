<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('published_listings', function (Blueprint $table): void {
            $table->string('text_scan_grade', 16)->nullable()->after('text_condition_grade');
            $table->boolean('text_scan_checked')->default(false)->after('text_scan_grade')->index();
            $table->boolean('text_scan_review_required')->default(false)->after('text_scan_checked');
            $table->json('text_scan_risk_flags')->nullable()->after('text_scan_review_required');
            $table->string('text_scan_model_name')->nullable()->after('text_scan_risk_flags');
            $table->timestampTz('text_scan_checked_at')->nullable()->after('text_scan_model_name');
        });

        foreach (['published_comps', 'published_sold_evidence'] as $tableName) {
            Schema::table($tableName, function (Blueprint $table): void {
                $table->string('text_scan_grade', 16)->nullable()->after('model_key');
                $table->boolean('text_scan_checked')->default(false)->after('text_scan_grade')->index();
                $table->boolean('text_scan_review_required')->default(false)->after('text_scan_checked');
                $table->json('text_scan_risk_flags')->nullable()->after('text_scan_review_required');
                $table->string('text_scan_model_name')->nullable()->after('text_scan_risk_flags');
                $table->timestampTz('text_scan_checked_at')->nullable()->after('text_scan_model_name');
            });
        }

        Schema::table('published_model_key_comp_rows', function (Blueprint $table): void {
            $table->string('text_scan_grade', 16)->nullable()->after('bids_count');
            $table->boolean('text_scan_checked')->default(false)->after('text_scan_grade')->index();
            $table->boolean('text_scan_review_required')->default(false)->after('text_scan_checked');
            $table->json('text_scan_risk_flags')->nullable()->after('text_scan_review_required');
            $table->string('text_scan_model_name')->nullable()->after('text_scan_risk_flags');
            $table->timestampTz('text_scan_checked_at')->nullable()->after('text_scan_model_name');
        });

        Schema::table('listing_favourites', function (Blueprint $table): void {
            $table->string('text_scan_grade', 16)->nullable()->after('text_condition_grade');
            $table->boolean('text_scan_checked')->default(false)->after('text_scan_grade')->index();
            $table->boolean('text_scan_review_required')->default(false)->after('text_scan_checked');
            $table->json('text_scan_risk_flags')->nullable()->after('text_scan_review_required');
            $table->string('text_scan_model_name')->nullable()->after('text_scan_risk_flags');
            $table->timestampTz('text_scan_checked_at')->nullable()->after('text_scan_model_name');
        });
    }

    public function down(): void
    {
        foreach (['listing_favourites', 'published_model_key_comp_rows', 'published_comps', 'published_sold_evidence', 'published_listings'] as $tableName) {
            Schema::table($tableName, function (Blueprint $table): void {
                $table->dropColumn([
                    'text_scan_grade',
                    'text_scan_checked',
                    'text_scan_review_required',
                    'text_scan_risk_flags',
                    'text_scan_model_name',
                    'text_scan_checked_at',
                ]);
            });
        }
    }
};
