<?php

namespace App\Notifications;

use App\Models\ListingFavourite;
use App\Models\PublishedListing;
use App\Support\SourceCatalog;
use Illuminate\Bus\Queueable;
use Illuminate\Notifications\Messages\MailMessage;
use Illuminate\Notifications\Notification;
use Illuminate\Support\Carbon;
use Illuminate\Support\Str;

class FavouriteEndingSoonNotification extends Notification
{
    use Queueable;

    public function __construct(
        public ListingFavourite $favourite,
        public ?PublishedListing $currentListing = null,
    ) {}

    /**
     * @return list<string>
     */
    public function via(object $notifiable): array
    {
        return ['mail'];
    }

    public function toMail(object $notifiable): MailMessage
    {
        $title = $this->text('title');
        $endTime = $this->date('end_time');
        $currentPrice = $this->money('current_price');
        $expectedProfit = $this->money('expected_profit');
        $maxSafeBuy = $this->money('max_safe_buy');
        $soldMedian = $this->money('sold_median');
        $roi = (float) $this->value('expected_roi');
        $compSamples = (int) $this->value('comp_samples');
        $grade = $this->text('condition_grade');
        $saleType = $this->text('sale_type') ?: 'listing';
        $source = SourceCatalog::label($this->text('source'));
        $externalId = $this->text('external_id');
        $favouriteUrl = route('favourites.show', $this->favourite);
        $ebayUrl = $this->text('url');

        $message = (new MailMessage)
            ->subject('Ending soon: '.Str::limit($title, 72, '…'))
            ->greeting('Ending soon')
            ->line($title)
            ->line($source.' · '.$saleType.' · Item '.$externalId)
            ->line('Current price: '.$currentPrice)
            ->line('Expected profit: '.$expectedProfit.' · '.number_format($roi * 100).'% ROI')
            ->line('Max safe buy: '.$maxSafeBuy)
            ->line('Sold median: '.$soldMedian.' from '.number_format($compSamples).' comps')
            ->line('Grade: '.$grade)
            ->action('View favourite details', $favouriteUrl);

        if ($endTime instanceof Carbon) {
            $message->line('Ends '.$endTime->timezone(config('app.timezone'))->format('D j M Y H:i').' · '.$endTime->diffForHumans());
        }

        if ($ebayUrl !== '') {
            $message->line('Live listing: '.$ebayUrl);
        }

        return $message;
    }

    private function value(string $attribute): mixed
    {
        return $this->currentListing?->{$attribute} ?? $this->favourite->{$attribute};
    }

    private function text(string $attribute): string
    {
        return (string) ($this->value($attribute) ?? '');
    }

    private function date(string $attribute): ?Carbon
    {
        $value = $this->value($attribute);

        if ($value instanceof Carbon) {
            return $value;
        }

        return $value ? Carbon::parse($value) : null;
    }

    private function money(string $attribute): string
    {
        return '£'.number_format((float) $this->value($attribute), 2);
    }
}
