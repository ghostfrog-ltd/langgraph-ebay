@props([
    'grade' => null,
    'compact' => false,
])

@php
    $label = 'Effective '.($grade ?: 'unknown');
@endphp

<span
    title="Condition grade used for comp matching"
    {{ $attributes->class([
        'inline-flex items-center rounded-full bg-zinc-950 font-bold uppercase tracking-wider text-white ring-1 ring-zinc-950/10 dark:bg-white dark:text-zinc-950 dark:ring-white/10',
        $compact ? 'px-2 py-0.5 text-[10px]' : 'px-2.5 py-1 text-[11px]',
    ]) }}
>
    {{ $label }}
</span>
