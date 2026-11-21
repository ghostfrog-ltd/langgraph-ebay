-- Retain public eBay paths and numeric variation IDs. Strip tracking and
-- authentication queries and reject known credential markers in the path.
CREATE OR REPLACE FUNCTION public.ghostfrog_marketplace_url(input text, image boolean)
RETURNS text LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $function$
    WITH parsed AS (
        SELECT split_part(split_part(input, '?', 1), '#', 1) AS path
    )
    SELECT CASE
        WHEN path ~* '(sk[-_][a-z0-9_-]{12,}|gh[pousr]_[a-z0-9_]{12,}|github_pat_|xox[baprs]-|AKIA[a-z0-9]{16}|eyJ[a-z0-9_-]+[.][a-z0-9_-]+[.][a-z0-9_-]+)'
          OR path ~ '/[.][.]?(/|$)' THEN NULL
        WHEN image AND path ~ '^https?://i[.]ebayimg[.]com/(?:images/g/|thumbs/images/g/)[A-Za-z0-9_~./-]+$'
        THEN path
        WHEN NOT image AND path
             ~ '^https?://(?:www[.])?ebay[.](?:co[.]uk|com|com[.]au|ca|de|fr|it|es|ie|nl|at|ch|be|pl)/itm/(?:[A-Za-z0-9_.~-]+/)?[0-9]{12}/?$'
        THEN path || COALESCE('?var=' || substring(input FROM '[?&]var=([0-9]{1,12})(?:[&#]|$)'), '')
    END
    FROM parsed
$function$;
