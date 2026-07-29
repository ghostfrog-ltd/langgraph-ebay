-- Allow one comp snapshot per product identity and condition grade.

ALTER TABLE comps
    DROP CONSTRAINT IF EXISTS comps_pkey;

ALTER TABLE comps
    ADD CONSTRAINT comps_pkey PRIMARY KEY (model_key, condition_grade);
