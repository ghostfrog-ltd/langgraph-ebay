@props([
    'checked' => false,
    'grade' => null,
    'review' => false,
    'compact' => false,
])

@php
    $isChecked = (bool) $checked;
    $needsReview = (bool) $review;
    $label = $isChecked
        ? 'Text scan '.($grade ?: 'unknown').($needsReview ? ' review' : '')
        : 'Text scan unchecked';
    $classes = ! $isChecked
        ? 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300'
        : ($needsReview
            ? 'bg-amber-100 text-amber-800 dark:bg-amber-950 dark:text-amber-300'
            : 'bg-sky-100 text-sky-800 dark:bg-sky-950 dark:text-sky-300');
@endphp

<span {{ $attributes->class([
    'inline-flex items-center rounded-full font-bold uppercase tracking-wider',
    $compact ? 'px-2 py-0.5 text-[10px]' : 'px-2.5 py-1 text-[11px]',
    $classes,
]) }}>
    {{ $label }}
</span>
