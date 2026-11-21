-- Sanitize product text before it reaches the diagnostic role or can be
-- filtered, joined or split by client SQL. These helpers read no tables.
CREATE OR REPLACE FUNCTION public.ghostfrog_product_text(input text)
RETURNS text
LANGUAGE plpgsql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $function$
DECLARE
    value text := translate(input, '＠．', '@.');
    pattern text;
BEGIN
    value := regexp_replace(value, '(&#0*64;|&#x0*40;|&commat;|%40|\[[[:space:]]*at[[:space:]]*\]|\([[:space:]]*at[[:space:]]*\))', '@', 'gi');
    value := regexp_replace(value, '(&#0*46;|&#x0*2e;|&period;|%2e|\[[[:space:]]*dot[[:space:]]*\]|\([[:space:]]*dot[[:space:]]*\))', '.', 'gi');
    value := regexp_replace(value, $re$-----BEGIN ([A-Z0-9 ]+ )?PRIVATE KEY-----.*?(-----END ([A-Z0-9 ]+ )?PRIVATE KEY-----|$)$re$, '[redacted]', 'gi');
    value := regexp_replace(value, '<[^>]+>', ' ', 'g');

    FOREACH pattern IN ARRAY ARRAY[
        $re$(?:https?://|www\.)[^[:space:]<>|;]+$re$,
        $re$[[:alnum:]_.%+\-]+[[:blank:]]*@[[:blank:]]*[[:alnum:]\-]+(?:[[:blank:]]*\.[[:blank:]]*[[:alnum:]\-]+)+$re$,
        $re$(?<![[:alnum:]])@[A-Za-z][A-Za-z0-9_.-]{2,}\M$re$,
        $re$\m(?:authorization|proxy-authorization|cookie|set-cookie|x-api-key)\M[[:blank:]]*[:=][^|\r\n]*$re$,
        $re$\m(?:api[ _-]?key|access[ _-]?token|refresh[ _-]?token|session[ _-]?token|client[ _-]?secret|password|passwd|secret|token)\M[[:blank:]]*[:=][^|,;\r\n]*$re$,
        $re$\m(?:bearer|basic)[[:blank:]]+[A-Za-z0-9._+/=\-]{8,}$re$,
        $re$\m(?:sk[-_][A-Za-z0-9_-]{12,}|gh[pousr]_[A-Za-z0-9_]{12,}|github_pat_[A-Za-z0-9_]{12,}|xox[baprs]-[A-Za-z0-9-]{10,}|(?:AKIA|ASIA)[A-Z0-9]{16}|eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+)\M$re$,
        $re$\m(?=[A-Za-z0-9_-]{24,}\M)(?=[A-Za-z0-9_-]*[0-9])[A-Za-z0-9_-]+\M$re$,
        $re$\m(?:(?:seller|buyer|owner|contact|recipient)[ _-]+(?:name|username|user[ _-]?id|email|phone|address|details)|seller|buyer|owner|contact|recipient|username|e-?mail|mobile|telephone|phone|whatsapp|telegram|shipping[ _-]+address|return[ _-]+address|postal[ _-]+address|address|postcode|zip[ _-]+code|bank[ _-]+details|iban|sort[ _-]+code|account[ _-]+number)\M[[:blank:]]*[:=][^|;\r\n]*$re$,
        $re$\m(?:collect(?:ion)?[[:blank:]]+(?:from|at|in)|meet[[:blank:]]+(?:me[[:blank:]]+)?at|seller[[:blank:]]+is|sold[[:blank:]]+by|(?:call|text|message|contact)[[:blank:]]+(?:me|us)[[:blank:]]+(?:at|on))\M[[:blank:]]+[^|;\r\n]*$re$,
        $re$\m(?:serial(?:[[:blank:]]+(?:number|no\.?))?|IMEI(?:[[:blank:]]+[12])?|ICCID|EID|UDID)\M[[:blank:]]*(?:[:=#-][[:blank:]]*|[[:blank:]]+is[[:blank:]]+|[[:blank:]]+)[A-Za-z0-9-]{6,}\M$re$,
        $re$\m(?:\d{1,5}[A-Z]?[[:blank:]]+(?:[[:alnum:].'’-]+[[:blank:]]+){1,5}(?:street|st|road|rd|avenue|ave|lane|ln|drive|dr|close|way|court|ct|terrace|place|pl|boulevard|blvd|crescent|square|gardens|grove))\M\.?$re$,
        $re$\m(?!(?:M[1-9]|A[0-9]{1,2})[[:blank:]]+[0-9](?:GB|TB|MB)\M)(?:GIR[[:blank:]]*0AA|[A-Z]{1,2}[0-9][0-9A-Z]?[[:blank:]]+[0-9][A-Z]{2})\M$re$,
        $re$(?<![[:alnum:]])(?:\+?[0-9]|\([0-9])[0-9(). -]{7,}[0-9](?![[:alnum:]])$re$,
        $re$\m(?:[0-9]{1,3}\.){3}[0-9]{1,3}\M$re$
    ] LOOP
        value := regexp_replace(value, pattern, '[redacted]', 'gi');
    END LOOP;

    RETURN btrim(regexp_replace(value, '[[:cntrl:]]+', ' ', 'g'));
END
$function$;

CREATE OR REPLACE FUNCTION public.ghostfrog_apple_evidence(title_text text, item_specifics jsonb)
RETURNS TABLE (
    evidence_seq bigint, field text, value_index bigint,
    part_index integer, part_count integer, product_value text
)
LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog
AS $function$
    WITH RECURSIVE values_at_path(field, path, value) AS (
        SELECT 'title'::text, ARRAY[]::bigint[], to_jsonb(title_text)
        UNION ALL
        SELECT j.key, ARRAY[]::bigint[], j.value
        FROM jsonb_each(CASE WHEN jsonb_typeof(item_specifics) = 'object'
                            THEN item_specifics ELSE '{}'::jsonb END) AS j
        WHERE (CASE WHEN lower(j.key) LIKE 'browse aspect %' THEN lower(substr(j.key, 15))
                    ELSE lower(j.key) END) IN (
            'brand', 'model', 'model name', 'model number', 'model identifier', 'apple model',
            'mpn', 'manufacturer part number', 'product line', 'series', 'type', 'variant',
            'generation', 'release year', 'year of manufacture', 'screen size', 'display',
            'display type', 'maximum resolution', 'processor', 'processor model', 'processor speed',
            'chip', 'chipset model', 'cpu', 'gpu', 'graphics processing type', 'ram', 'ram size',
            'memory', 'memory size', 'unified memory', 'storage capacity', 'ssd capacity',
            'hard drive capacity', 'storage type', 'capacity', 'connectivity', 'internet connectivity',
            'network connectivity', 'network', 'carrier', 'cellular band', 'sim card slot',
            'lock status', 'activation lock', 'icloud lock', 'carrier lock', 'network lock',
            'condition', 'browse condition', 'browse conditiondescription', 'condition description',
            'conditiondescription', 'item condition', 'cosmetic condition', 'condition/grade',
            'grade', 'defect', 'defects', 'defect description', 'damage', 'fault', 'faults', 'parts',
            'parts or repair', 'repair',
            'features', 'battery health', 'battery capacity', 'case size', 'case material', 'material',
            'colour', 'color', 'manufacturer colour', 'unit quantity', 'pack quantity',
            'number in pack', 'number of items in set'
        )
        UNION ALL
        SELECT parent.field, parent.path || child.ordinal, child.value
        FROM values_at_path AS parent
        CROSS JOIN LATERAL (
            SELECT entry.value, entry.ordinal
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(parent.value) = 'array'
                                          THEN parent.value ELSE '[]'::jsonb END)
                 WITH ORDINALITY AS entry(value, ordinal)
            UNION ALL
            SELECT parent.value -> wrapper.key, wrapper.ordinal
            FROM (VALUES ('value', 1::bigint), ('values', 2::bigint)) AS wrapper(key, ordinal)
            WHERE jsonb_typeof(parent.value) = 'object' AND parent.value ? wrapper.key
        ) AS child
    ),
    sanitized AS MATERIALIZED (
        SELECT entry.field, entry.path, public.ghostfrog_product_text(entry.value #>> '{}') AS value
        FROM values_at_path AS entry
        WHERE jsonb_typeof(entry.value) IN ('string', 'number', 'boolean')
    ),
    numbered AS (
        SELECT item.field, item.value, row_number() OVER (
                   PARTITION BY item.field ORDER BY item.path
               ) AS value_index, (length(item.value) + 479) / 480 AS part_count
        FROM sanitized AS item
        WHERE item.value <> ''
    )
    SELECT row_number() OVER (ORDER BY item.field <> 'title', item.field COLLATE "C", item.value_index, part.n),
           item.field, item.value_index, part.n, item.part_count,
           substr(item.value, (part.n - 1) * 480 + 1, 480)
    FROM numbered AS item
    CROSS JOIN LATERAL generate_series(1, item.part_count) AS part(n)
$function$;

CREATE OR REPLACE VIEW public.apple_listing_evidence_v
WITH (security_barrier = true) AS
SELECT listing.id AS listing_id, listing.external_id, listing.source, listing.status,
       listing.model_key, listing.condition_grade, listing.final_price, listing.end_time,
       listing.last_seen_at, listing.fetched_at,
       evidence.evidence_seq, evidence.field, evidence.value_index,
       evidence.part_index, evidence.part_count, evidence.product_value
FROM public.auction_listings AS listing
CROSS JOIN LATERAL public.ghostfrog_apple_evidence(listing.title, listing.raw_attrs) AS evidence
WHERE listing.source = 'ebay-apple'
  AND listing.external_id ~ '^([0-9]{12}|v1[|][0-9]{12}[|][0-9]{1,12})$';

REVOKE ALL ON FUNCTION public.ghostfrog_product_text(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.ghostfrog_apple_evidence(text, jsonb) FROM PUBLIC;
REVOKE ALL ON public.apple_listing_evidence_v FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ghostfrog_product_text(text),
                          public.ghostfrog_apple_evidence(text, jsonb) TO ghostfrog_diagnostics;
GRANT SELECT ON public.apple_listing_evidence_v TO ghostfrog_diagnostics;
