-- Fixed, read-only projections for the single-item inspection tool. The
-- diagnostic login keeps its existing grants on the underlying relations.
CREATE OR REPLACE FUNCTION public.ghostfrog_marketplace_url(input text, image boolean)
RETURNS text LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $function$
    SELECT CASE
        WHEN image AND split_part(split_part(input, '?', 1), '#', 1)
             ~ '^https?://i[.]ebayimg[.]com/(?:images/g/|thumbs/images/g/)[A-Za-z0-9_~./-]+$'
        THEN split_part(split_part(input, '?', 1), '#', 1)
        WHEN NOT image AND split_part(split_part(input, '?', 1), '#', 1)
             ~ '^https?://(?:www[.])?ebay[.](?:co[.]uk|com|com[.]au|ca|de|fr|it|es|ie|nl|at|ch|be|pl)/itm/(?:[A-Za-z0-9_.~-]+/)?[0-9]{12}/?$'
        THEN split_part(split_part(input, '?', 1), '#', 1)
             || COALESCE('?var=' || substring(input FROM '[?&]var=([0-9]{1,12})(?:[&#]|$)'), '')
    END
$function$;

CREATE OR REPLACE FUNCTION public.ghostfrog_listing_product_field(label text, from_browse boolean)
RETURNS boolean LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $function$
    SELECT label IN ('model name', 'device name') OR (
        label ~ '^[a-z0-9][a-z0-9 (),/.+_-]{0,79}$'
             AND label !~ '(^|[^a-z0-9])(seller|buyer|recipient|account|address|postcode|zip|contact|e.?mail|telephone|phone|serial|imei|iccid|eid|uid|epid|url|token|key|password|secret|signature|auth|cookie|tracking|username|name|location|latitude|longitude|payment|payload|credential|metadata)([^a-z0-9]|$)'
             AND label !~ '^(browse |raw |misc|source|_|custom)'
             AND (from_browse OR label ~
                  '(^|[^a-z0-9])(brand|manufacturer|model|mpn|part|number|type|variant|series|generation|product|hardware|chip|processor|cpu|gpu|graphics|core|memory|ram|ssd|hdd|drive|storage|capacity|display|screen|resolution|colour|color|finish|dimension|size|weight|year|operating|firmware|software|connector|connectivity|network|carrier|band|sim|lock|activation|mdm|icloud|features|condition|grade|wear|battery|repair|fault|defect|damage|parts|bundle|pack|quantity|included|case|material|compatible|power|input|output|interface|speed|voltage|wattage|warranty|edition|language|audio|video|camera|lens|format|region|platform|game|set|pieces|ports|socket|mount|aspect|charging|release|gender|age|character|theme)([^a-z0-9]|$)')
    )
$function$;

CREATE OR REPLACE FUNCTION public.ghostfrog_listing_evidence(
    title_text text, notes_text text, attrs jsonb, assessments jsonb
)
RETURNS TABLE (
    section text, evidence_seq bigint, field text, value_path jsonb,
    part_index integer, part_count integer, value jsonb, withheld boolean
)
LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog
AS $function$
    WITH RECURSIVE
    original AS (
        SELECT j.key, j.value,
               CASE WHEN lower(j.key) LIKE 'browse aspect %' THEN lower(substr(j.key, 15))
                    ELSE lower(j.key) END AS label
        FROM jsonb_each(CASE WHEN jsonb_typeof(attrs) = 'object' THEN attrs ELSE '{}'::jsonb END) AS j
    ),
    classified AS (
        SELECT item.key, item.value, CASE
            WHEN item.label IN ('browse imageurl', 'browse imageurls') THEN 'photos'
            WHEN item.label IN (
                'browse seller username', 'browse seller feedbackscore', 'browse seller feedbackpercentage',
                'browse shippingoptions', 'browse returnterms returnsaccepted', 'browse returnterms refundmethod',
                'browse returnterms returnmethod', 'browse returnterms returnperiod',
                'browse returnterms returnshippingcostpayer', 'browse itemlocation country',
                'browse itemlocation city', 'browse itemlocation stateorprovince', 'browse condition',
                'browse buyingoptions', 'browse estimatedavailabilities', 'browse availability',
                'browse itemcreationdate', 'browse itemenddate'
            ) THEN 'seller_terms'
            WHEN item.label IN (
                'description', 'short description', 'shortdescription', 'subtitle', 'conditiondescription',
                'condition description', 'browse description', 'browse shortdescription',
                'browse subtitle', 'browse conditiondescription'
            ) THEN 'description'
            WHEN public.ghostfrog_listing_product_field(item.label, lower(item.key) LIKE 'browse aspect %')            THEN 'product'

        END AS section
        FROM original AS item
    ),
    tree(section, field, path, position, input) AS (
        SELECT item.section, item.key, jsonb_build_array(item.key), ARRAY[]::bigint[], item.value
        FROM classified AS item WHERE item.section IS NOT NULL
        UNION ALL
        SELECT 'description', text.key, jsonb_build_array(text.key), ARRAY[]::bigint[], to_jsonb(text.value)
        FROM (VALUES ('title', title_text), ('notes', notes_text)) AS text(key, value)
        WHERE text.value IS NOT NULL
        UNION ALL
        SELECT 'checks', check_entry.key, jsonb_build_array(check_entry.key), ARRAY[]::bigint[], check_entry.value
        FROM jsonb_each(COALESCE(assessments, '{}'::jsonb)) AS check_entry
        UNION ALL
        SELECT parent.section, parent.field, parent.path || child.label, parent.position || child.ordinal, child.value
        FROM tree AS parent
        CROSS JOIN LATERAL (
            SELECT to_jsonb(a.ordinal - 1) AS label, a.ordinal, a.value
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(parent.input) = 'array'
                                          THEN parent.input ELSE '[]'::jsonb END)
                 WITH ORDINALITY AS a(value, ordinal)
            UNION ALL
            SELECT to_jsonb(j.key), row_number() OVER (ORDER BY j.key COLLATE "C"), j.value
            FROM jsonb_each(CASE WHEN jsonb_typeof(parent.input) = 'object'
                                 THEN parent.input ELSE '{}'::jsonb END) AS j
            WHERE (
                (parent.section = 'photos' AND j.key IN ('imageUrl', 'url'))
                OR (parent.section = 'seller_terms' AND parent.field IN
                    ('Browse shippingOptions', 'Browse estimatedAvailabilities')
                    AND j.key IN ('shippingServiceCode', 'shippingCarrierCode', 'type', 'shippingCost',
                                  'value', 'currency', 'minEstimatedDeliveryDate', 'maxEstimatedDeliveryDate',
                                  'estimatedAvailabilityStatus', 'estimatedAvailableQuantity', 'availabilityThreshold'))
                OR (parent.section = 'checks' AND j.key IN
                    ('notes', 'risk_reasons', 'facts', 'condition_normalized', 'price_analysis',
                     'condition', 'missing_parts', 'repair', 'lock_status', 'image_count', 'elapsed_s',
                     'shipping', 'fees', 'estimated_resale', 'min_profit', 'expected_profit', 'value', 'values'))
                OR (parent.section = 'product' AND public.ghostfrog_listing_product_field(lower(j.key), false))
                OR (parent.section IN ('product', 'description') AND j.key IN ('value', 'values') AND NOT EXISTS (
                    SELECT 1
                    FROM (VALUES ('name'), ('key'), ('label')) AS descriptor(key)
                    WHERE parent.input ? descriptor.key
                      AND lower(parent.field) IS DISTINCT FROM lower(parent.input ->> descriptor.key)
                      AND lower(parent.field) IS DISTINCT FROM ('browse aspect ' || lower(parent.input ->> descriptor.key))
                ))
            )
        ) AS child
    ),
    safe AS MATERIALIZED (
        SELECT t.section, t.field, t.path, t.position, jsonb_typeof(t.input) AS kind,
               CASE
                   WHEN t.section = 'photos' THEN public.ghostfrog_marketplace_url(t.input #>> '{}', true)
                   WHEN lower(t.field) = 'browse seller username' THEN
                       CASE WHEN t.input #>> '{}' ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$'
                            AND public.ghostfrog_product_text(t.input #>> '{}') = t.input #>> '{}'
                            THEN t.input #>> '{}' END
                   WHEN lower(t.field) LIKE 'browse itemlocation %' THEN
                       CASE WHEN t.input #>> '{}' ~ '^[[:alpha:]][[:alpha:] .''-]{0,63}$'
                            THEN public.ghostfrog_product_text(t.input #>> '{}') END
                   WHEN jsonb_typeof(t.input) = 'string' THEN public.ghostfrog_product_text(t.input #>> '{}')
                   WHEN t.section = 'product' AND t.input #>> '{}' ~ '^[0-9]{9,}$' THEN NULL
                   ELSE t.input #>> '{}'
               END AS text_value
        FROM tree AS t
        WHERE jsonb_typeof(t.input) IN ('string', 'number', 'boolean')
    ),
    numbered AS (
        SELECT s.*, GREATEST(1, (COALESCE(length(s.text_value), 0) + 479) / 480) AS parts
        FROM safe AS s
        WHERE s.text_value IS DISTINCT FROM ''
    )
    SELECT n.section,
           row_number() OVER (PARTITION BY n.section ORDER BY n.field <> 'title', n.field COLLATE "C", n.position, p.part),
           n.field, n.path, p.part, n.parts,
           CASE WHEN n.kind IN ('number', 'boolean') AND n.section <> 'photos' THEN n.text_value::jsonb
                ELSE to_jsonb(substr(n.text_value, (p.part - 1) * 480 + 1, 480)) END,
           n.text_value IS NULL
    FROM numbered AS n CROSS JOIN LATERAL generate_series(1, n.parts) AS p(part)
$function$;

CREATE OR REPLACE FUNCTION public.ghostfrog_listing_identity_risk(title_text text, attrs jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog
AS $function$
    SELECT (coalesce(title_text, '') || ' ' || coalesce(attrs::text, '')) ~* $re$\m(for parts|parts only|for spares|spares only|spares or repair|parts or repair|repair only|needs repair|faulty|not working|untested|water damage|activation locked|icloud locked|mdm locked|locked to owner|locked|empty box|box only|case only|replacement screen)\M$re$
$function$;

CREATE OR REPLACE VIEW public.listing_inspection_v
WITH (security_barrier = true) AS
SELECT a.id AS listing_id, a.external_id, a.source,
       CASE WHEN a.external_id LIKE 'v1|%' THEN split_part(a.external_id, '|', 2) ELSE a.external_id END AS item_id,
       public.ghostfrog_product_text(a.title) AS title,
       public.ghostfrog_marketplace_url(a.url, false) AS url,
       a.url IS NOT NULL AND public.ghostfrog_marketplace_url(a.url, false) IS NULL AS url_withheld,
       public.ghostfrog_product_text(a.sale_type) AS sale_type,
       public.ghostfrog_product_text(a.status) AS status, a.finalized, a.first_seen, a.last_seen_at, a.fetched_at,
       a.price_current, a.price_bid_current, a.bids_count, a.end_time, a.condition_grade,
       CASE WHEN a.status = 'sold' AND a.final_price > 0 AND a.end_time <= now() THEN a.final_price END AS final_price,
       CASE WHEN a.model_key ~ '^[A-Za-z0-9][A-Za-z0-9_.+-]{0,159}$'
             AND a.model_key !~* '^(sk[-_]|bearer|gh[pousr]_|github_pat_|xox[baprs]-|AKIA[0-9A-Z]|eyJ[A-Za-z0-9_-])'
            THEN a.model_key END AS model_key, a.storage_gb,
       public.ghostfrog_listing_identity_risk(a.title, a.raw_attrs) AS identity_risk,
       public.ghostfrog_product_text(a.brand) AS brand, public.ghostfrog_product_text(a.product_family) AS product_family,
       public.ghostfrog_product_text(a.model_name) AS model_name, public.ghostfrog_product_text(a.colour) AS colour,
       CASE WHEN a.roi_estimate NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)
            THEN a.roi_estimate END AS roi_estimate,
       a.max_bid, a.backfill_checked_at, a.backfill_check_count,
       public.ghostfrog_product_text(a.backfill_last_reason) AS backfill_last_reason,
       a.text_condition_checked, a.text_condition_checked_at, a.visual_condition_checked, a.visual_condition_checked_at,
       CASE WHEN a.raw_attrs ? 'Browse description' THEN length(a.raw_attrs ->> 'Browse description') END AS browse_description_chars,
       CASE WHEN jsonb_typeof(a.raw_attrs -> 'Browse imageUrls') = 'array'
            THEN jsonb_array_length(a.raw_attrs -> 'Browse imageUrls') END AS stored_gallery_count,
       CASE WHEN jsonb_typeof(a.raw_attrs -> 'Browse shippingOptions') = 'array'
            THEN jsonb_array_length(a.raw_attrs -> 'Browse shippingOptions') END AS stored_shipping_option_count,
       tc.text_grade, vc.visual_grade,
       CASE
           WHEN 'D' IN (upper(COALESCE(a.condition_grade, 'B')), upper(tc.text_grade), upper(vc.visual_grade)) THEN 'D'
           WHEN 'C' IN (upper(COALESCE(a.condition_grade, 'B')), upper(tc.text_grade), upper(vc.visual_grade)) THEN 'C'
           WHEN 'B' IN (upper(COALESCE(a.condition_grade, 'B')), upper(tc.text_grade), upper(vc.visual_grade)) THEN 'B'
           ELSE 'A'
       END AS effective_condition_grade,
       tc.review_required AS text_review_required, vc.review_required AS visual_review_required,
       a.source_id
FROM public.auction_listings AS a
LEFT JOIN public.latest_listing_text_conditions AS tc ON tc.listing_id = a.id
LEFT JOIN public.latest_listing_visual_conditions AS vc ON vc.listing_id = a.id
WHERE a.source ~ '^ebay-[a-z0-9-]+$'
  AND a.external_id ~ '^([0-9]{12}|v1[|][0-9]{12}[|][0-9]{1,12})$';

CREATE OR REPLACE VIEW public.listing_inspection_evidence_v
WITH (security_barrier = true) AS
SELECT a.id AS listing_id, e.*, e.value #>> '{}' AS value_text, e.value_path::text AS value_path_text
FROM public.auction_listings AS a
LEFT JOIN public.latest_listing_text_conditions AS tc ON tc.listing_id = a.id
LEFT JOIN public.latest_listing_visual_conditions AS vc ON vc.listing_id = a.id
LEFT JOIN LATERAL (
    SELECT x.assessment FROM public.listing_assessments AS x
    WHERE x.listing_id = a.id ORDER BY x.created_at DESC, x.id DESC LIMIT 1
) AS ass ON true
CROSS JOIN LATERAL public.ghostfrog_listing_evidence(
    a.title, a.notes, a.raw_attrs, jsonb_strip_nulls(
        jsonb_build_object('text assessment', tc.assessment, 'visual assessment', vc.assessment,
                           'listing assessment', ass.assessment, 'text risk flags', to_jsonb(tc.risk_flags),
                           'visual damage flags', to_jsonb(vc.damage_flags))
    )
) AS e
WHERE a.source ~ '^ebay-[a-z0-9-]+$'
  AND a.external_id ~ '^([0-9]{12}|v1[|][0-9]{12}[|][0-9]{1,12})$';

CREATE OR REPLACE VIEW public.listing_inspection_checks_v
WITH (security_barrier = true) AS
SELECT a.id AS listing_id, check_entry.kind, check_entry.checked_at, check_entry.grade,
       check_entry.confidence, check_entry.review_required, check_entry.model_name,
       check_entry.flags, check_entry.verdict, check_entry.recommended_max_bid
FROM public.auction_listings AS a
CROSS JOIN LATERAL (
    SELECT 'text'::text AS kind, t.checked_at AT TIME ZONE 'UTC' AS checked_at, t.text_grade AS grade,
           t.confidence, t.review_required, public.ghostfrog_product_text(t.model_name) AS model_name,
           ARRAY(SELECT public.ghostfrog_product_text(flag) FROM unnest(t.risk_flags) AS flag) AS flags,
           NULL::text AS verdict, NULL::numeric AS recommended_max_bid
    FROM public.latest_listing_text_conditions AS t WHERE t.listing_id = a.id
    UNION ALL
    SELECT 'visual', v.checked_at AT TIME ZONE 'UTC', v.visual_grade,
           v.confidence, v.review_required, public.ghostfrog_product_text(v.model_name),
           ARRAY(SELECT public.ghostfrog_product_text(flag) FROM unnest(v.damage_flags) AS flag), NULL, NULL
    FROM public.latest_listing_visual_conditions AS v WHERE v.listing_id = a.id
    UNION ALL
    (SELECT 'listing_assessment', x.created_at, NULL, x.confidence::numeric, NULL, NULL, NULL,
            public.ghostfrog_product_text(x.verdict), x.recommended_max_bid
     FROM public.listing_assessments AS x WHERE x.listing_id = a.id
     ORDER BY x.created_at DESC, x.id DESC LIMIT 1)
) AS check_entry
WHERE a.source ~ '^ebay-[a-z0-9-]+$'
  AND a.external_id ~ '^([0-9]{12}|v1[|][0-9]{12}[|][0-9]{1,12})$';

-- Use the indexed stored key before applying the safe projection. No raw
-- title or attribute column is returned. This is current matching evidence;
-- the materialized comp snapshot stores statistics, not historical row IDs.
CREATE OR REPLACE FUNCTION public.ghostfrog_listing_comparables(
    target_id integer, page_offset integer, page_limit integer
)
RETURNS TABLE (
    listing_id integer, item_id text, title text, url text, model_key text,
    condition_grade text, final_price numeric, end_time timestamptz,
    text_review_required boolean, visual_review_required boolean
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
    WITH target AS MATERIALIZED (
        SELECT t.listing_id, t.item_id, t.source, t.model_key, t.effective_condition_grade
        FROM public.listing_inspection_v AS t
        WHERE t.listing_id = target_id
          AND t.model_key ~ '^[a-z0-9][a-z0-9.-]+$'
          AND t.model_key !~ '(^unknown|generic|gen-unk)'
          AND t.effective_condition_grade IN ('A', 'B', 'C')
    )
    SELECT a.id, identifier.item_id, public.ghostfrog_product_text(a.title),
           public.ghostfrog_marketplace_url(a.url, false), a.model_key,
           grade.effective, a.final_price, a.end_time, tc.review_required, vc.review_required
    FROM target AS t
    JOIN public.auction_listings AS a ON a.model_key = t.model_key AND a.source = t.source
    CROSS JOIN LATERAL (
        SELECT CASE WHEN a.external_id LIKE 'v1|%' THEN split_part(a.external_id, '|', 2)
                    ELSE a.external_id END AS item_id
    ) AS identifier
    LEFT JOIN LATERAL (
        SELECT c.text_grade, c.review_required FROM public.listing_text_conditions AS c
        WHERE c.listing_id = a.id ORDER BY c.checked_at DESC, c.id DESC LIMIT 1
    ) AS tc ON true
    LEFT JOIN LATERAL (
        SELECT c.visual_grade, c.review_required FROM public.listing_visual_conditions AS c
        WHERE c.listing_id = a.id ORDER BY c.checked_at DESC, c.id DESC LIMIT 1
    ) AS vc ON true
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN 'D' IN (upper(COALESCE(a.condition_grade, 'B')), upper(tc.text_grade), upper(vc.visual_grade)) THEN 'D'
            WHEN 'C' IN (upper(COALESCE(a.condition_grade, 'B')), upper(tc.text_grade), upper(vc.visual_grade)) THEN 'C'
            WHEN 'B' IN (upper(COALESCE(a.condition_grade, 'B')), upper(tc.text_grade), upper(vc.visual_grade)) THEN 'B'
            ELSE 'A' END AS effective
    ) AS grade
    WHERE a.status = 'sold' AND a.finalized IS TRUE AND a.final_price > 0
      AND a.end_time BETWEEN now() - interval '365 days' AND now()
      AND a.external_id ~ '^([0-9]{12}|v1[|][0-9]{12}[|][0-9]{1,12})$'
      AND grade.effective = t.effective_condition_grade AND identifier.item_id <> t.item_id
      AND NOT COALESCE(tc.review_required, false) AND NOT COALESCE(vc.review_required, false)
      AND NOT public.ghostfrog_listing_identity_risk(a.title, a.raw_attrs)
      AND COALESCE((SELECT r.action FROM public.listing_review_actions AS r
                    WHERE r.external_id = a.external_id ORDER BY r.created_at DESC, r.id DESC LIMIT 1), '') <> 'down'
    ORDER BY a.end_time DESC, a.id DESC
    LIMIT LEAST(GREATEST(COALESCE(page_limit, 0), 0), 51)
    OFFSET LEAST(GREATEST(COALESCE(page_offset, 0), 0), 10000)
$function$;

REVOKE ALL ON FUNCTION public.ghostfrog_marketplace_url(text, boolean),
                       public.ghostfrog_listing_evidence(text, text, jsonb, jsonb) FROM PUBLIC;
REVOKE ALL ON public.listing_inspection_v, public.listing_inspection_evidence_v,
              public.listing_inspection_checks_v FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ghostfrog_marketplace_url(text, boolean),
                          public.ghostfrog_listing_evidence(text, text, jsonb, jsonb) TO ghostfrog_diagnostics;
GRANT SELECT ON public.listing_inspection_v, public.listing_inspection_evidence_v,
                public.listing_inspection_checks_v TO ghostfrog_diagnostics;

REVOKE ALL ON FUNCTION public.ghostfrog_listing_comparables(integer, integer, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ghostfrog_listing_comparables(integer, integer, integer) TO ghostfrog_diagnostics;

REVOKE ALL ON FUNCTION public.ghostfrog_listing_product_field(text, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ghostfrog_listing_product_field(text, boolean) TO ghostfrog_diagnostics;

REVOKE ALL ON FUNCTION public.ghostfrog_listing_identity_risk(text, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.ghostfrog_listing_identity_risk(text, jsonb) TO ghostfrog_diagnostics;
