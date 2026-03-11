<?php

namespace Database\Factories;

use App\Models\EbayListing;
use Illuminate\Database\Eloquent\Factories\Factory;

/**
 * @extends Factory<EbayListing>
 */
class EbayListingFactory extends Factory
{
    protected $model = EbayListing::class;

    public function definition(): array
    {
        $currentPrice = fake()->randomFloat(2, 50, 500);
        $estimatedMarketValue = round($currentPrice * fake()->randomFloat(2, 0.9, 1.6), 2);

        return [
            'ebay_id' => 'EBAY-'.fake()->unique()->numerify('##########'),
            'source_platform' => fake()->randomElement(['eBay UK', 'eBay US']),
            'category_name' => fake()->randomElement(['Apple', 'Consoles', 'Lego', 'Watches']),
            'title' => fake()->sentence(6),
            'image_url' => fake()->optional()->imageUrl(640, 640),
            'current_price' => $currentPrice,
            'estimated_market_value' => $estimatedMarketValue,
            'item_url' => fake()->url(),
            'llm_analysis' => fake()->optional()->paragraph(),
            'attributes_json' => [
                'condition' => fake()->randomElement(['Used', 'Refurbished', 'New']),
                'location' => fake()->city(),
                'seller_feedback_score' => fake()->numberBetween(90, 10000),
            ],
            'pipeline_state' => fake()->randomElement(['ingested', 'analyzing', 'opportunity', 'dismissed']),
        ];
    }
}
