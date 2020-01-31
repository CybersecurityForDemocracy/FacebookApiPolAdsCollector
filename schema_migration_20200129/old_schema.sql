DROP SEQUENCE IF EXISTS public.ad_categories_id_seq;

CREATE SEQUENCE public.ad_categories_id_seq
    INCREMENT 1
    START 10
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.ad_categories_id_seq
    OWNER TO nyufbpolads;

DROP SEQUENCE IF EXISTS public.ad_sponsor_id_seq;

CREATE SEQUENCE public.ad_sponsor_id_seq
    INCREMENT 1
    START 100306
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.ad_sponsor_id_seq
    OWNER TO nyufbpolads;

DROP SEQUENCE IF EXISTS public.ads_nyu_id_seq;

CREATE SEQUENCE public.ads_nyu_id_seq
    INCREMENT 1
    START 7729338
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.ads_nyu_id_seq
    OWNER TO nyufbpolads;

DROP SEQUENCE IF EXISTS public.demo_groups_id_seq;

CREATE SEQUENCE public.demo_groups_id_seq
    INCREMENT 1
    START 84409
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.demo_groups_id_seq
    OWNER TO nyufbpolads;

DROP SEQUENCE IF EXISTS public.demo_impressions_nyu_id1_seq;

CREATE SEQUENCE public.demo_impressions_nyu_id1_seq
    INCREMENT 1
    START 480199786
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.demo_impressions_nyu_id1_seq
    OWNER TO nyufbpolads;

DROP SEQUENCE IF EXISTS public.impressions_nyu_id1_seq;

CREATE SEQUENCE public.impressions_nyu_id1_seq
    INCREMENT 1
    START 47633071
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.impressions_nyu_id1_seq
    OWNER TO nyufbpolads;

DROP SEQUENCE IF EXISTS public.region_impressions_nyu_id1_seq;

CREATE SEQUENCE public.region_impressions_nyu_id1_seq
    INCREMENT 1
    START 1295460053
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.region_impressions_nyu_id1_seq
    OWNER TO nyufbpolads;

DROP SEQUENCE IF EXISTS public.regions_id_seq;

CREATE SEQUENCE public.regions_id_seq
    INCREMENT 1
    START 552842
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE public.regions_id_seq
    OWNER TO nyufbpolads;

-- Table: public.ad_categories

DROP TABLE IF EXISTS public.ad_categories;

CREATE TABLE public.ad_categories
(
    name character varying COLLATE pg_catalog."default",
    id integer NOT NULL DEFAULT nextval('ad_categories_id_seq'::regclass),
    CONSTRAINT ad_categories_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.ad_categories
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.ad_categories TO nyufbpolads;

-- Table: public.ad_link

DROP TABLE IF EXISTS public.ad_link;

CREATE TABLE public.ad_link
(
    ad_id bigint,
    archive_id bigint
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.ad_link
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.ad_link TO nyufbpolads;

-- Table: public.ad_scrape

DROP TABLE IF EXISTS public.ad_scrape;

CREATE TABLE public.ad_scrape
(
    ad_id bigint,
    archive_id bigint,
    image_url character varying COLLATE pg_catalog."default",
    video_url character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.ad_scrape
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.ad_scrape TO nyufbpolads;

-- Table: public.ad_sponsors

DROP TABLE IF EXISTS public.ad_sponsors;

CREATE TABLE public.ad_sponsors
(
    id integer NOT NULL DEFAULT nextval('ad_sponsor_id_seq'::regclass),
    name character varying(2000) COLLATE pg_catalog."default",
    federal_candidate boolean,
    nyu_category character varying COLLATE pg_catalog."default",
    partisan_lean character varying COLLATE pg_catalog."default",
    cmte_fec_id text COLLATE pg_catalog."default",
    parent_ad_sponsor_id integer,
    cmte_ein text COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.ad_sponsors
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.ad_sponsors TO nyufbpolads;

-- Table: public.ads

DROP TABLE IF EXISTS public.ads;

CREATE TABLE public.ads
(
    text text COLLATE pg_catalog."default",
    start_date timestamp(4) with time zone,
    end_date timestamp(4) with time zone,
    creation_date timestamp(4) with time zone,
    page_id bigint,
    currency character varying(255) COLLATE pg_catalog."default",
    snapshot_url character varying COLLATE pg_catalog."default",
    is_active boolean,
    ad_sponsor_id integer,
    archive_id bigint NOT NULL,
    nyu_id bigint NOT NULL DEFAULT nextval('ads_nyu_id_seq'::regclass),
    link_caption character varying COLLATE pg_catalog."default",
    link_description character varying COLLATE pg_catalog."default",
    link_title character varying COLLATE pg_catalog."default",
    ad_category_id integer,
    ad_id bigint,
    country_code character varying COLLATE pg_catalog."default",
    most_recent boolean,
    funding_entity character varying COLLATE pg_catalog."default",
    CONSTRAINT unique_ad_archive_id UNIQUE (archive_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.ads
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.ads TO nyufbpolads;

-- Table: public.demo_groups

DROP TABLE IF EXISTS public.demo_groups;

CREATE TABLE public.demo_groups
(
    age character varying(255) COLLATE pg_catalog."default",
    gender character varying(255) COLLATE pg_catalog."default",
    id integer NOT NULL DEFAULT nextval('demo_groups_id_seq'::regclass),
    CONSTRAINT demo_groups_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.demo_groups
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.demo_groups TO nyufbpolads;

-- Table: public.demo_impressions

DROP TABLE IF EXISTS public.demo_impressions;

CREATE TABLE public.demo_impressions
(
    ad_archive_id bigint NOT NULL,
    demo_id integer,
    min_impressions integer,
    max_impressions integer,
    min_spend integer,
    max_spend integer,
    crawl_date date,
    most_recent boolean,
    nyu_id bigint NOT NULL DEFAULT nextval('demo_impressions_nyu_id1_seq'::regclass),
    gender character varying COLLATE pg_catalog."default" NOT NULL,
    age character varying COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT archive_id_age_gender_unique PRIMARY KEY (ad_archive_id, age, gender),
    CONSTRAINT demo_impressions_unique_ad_archive_id UNIQUE (ad_archive_id, demo_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.demo_impressions
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.demo_impressions TO nyufbpolads;

-- Index: demo_impressions_archive_id_idx

DROP INDEX IF EXISTS public.demo_impressions_archive_id_idx;

CREATE INDEX demo_impressions_archive_id_idx
    ON public.demo_impressions USING btree
    (ad_archive_id ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.examples

DROP TABLE IF EXISTS public.examples;

CREATE TABLE public.examples
(
    ad_archive_id bigint
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.examples
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.examples TO nyufbpolads;

-- Table: public.images

DROP TABLE IF EXISTS public.images;

CREATE TABLE public.images
(
    image_url character varying COLLATE pg_catalog."default",
    image_phash character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.images
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.images TO nyufbpolads;

-- Table: public.impressions

DROP TABLE IF EXISTS public.impressions;

CREATE TABLE public.impressions
(
    ad_archive_id bigint,
    crawl_date date,
    min_impressions integer,
    min_spend integer,
    max_impressions integer,
    max_spend integer,
    most_recent boolean,
    nyu_id bigint NOT NULL DEFAULT nextval('impressions_nyu_id1_seq'::regclass),
    CONSTRAINT impressions_unique_ad_archive_id UNIQUE (ad_archive_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.impressions
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.impressions TO nyufbpolads;

-- Index: impressions_archive_id_idx

DROP INDEX IF EXISTS public.impressions_archive_id_idx;

CREATE INDEX impressions_archive_id_idx
    ON public.impressions USING btree
    (ad_archive_id ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.impressions

DROP TABLE IF EXISTS public.impressions;

CREATE TABLE public.impressions
(
    ad_archive_id bigint,
    crawl_date date,
    min_impressions integer,
    min_spend integer,
    max_impressions integer,
    max_spend integer,
    most_recent boolean,
    nyu_id bigint NOT NULL DEFAULT nextval('impressions_nyu_id1_seq'::regclass),
    CONSTRAINT impressions_unique_ad_archive_id UNIQUE (ad_archive_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.impressions
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.impressions TO nyufbpolads;

-- Index: impressions_archive_id_idx

DROP INDEX IF EXISTS public.impressions_archive_id_idx;

CREATE INDEX impressions_archive_id_idx
    ON public.impressions USING btree
    (ad_archive_id ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.multiple_ad_categories

DROP TABLE IF EXISTS public.multiple_ad_categories;

CREATE TABLE public.multiple_ad_categories
(
    ad_archive_id bigint,
    ad_category_id integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.multiple_ad_categories
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.multiple_ad_categories TO nyufbpolads;

-- Table: public.page_categories

DROP TABLE IF EXISTS public.page_categories;

CREATE TABLE public.page_categories
(
    page_id bigint,
    category character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.page_categories
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.page_categories TO nyufbpolads;

-- Table: public.pages

DROP TABLE IF EXISTS public.pages;

CREATE TABLE public.pages
(
    page_name character varying(255) COLLATE pg_catalog."default",
    page_id bigint,
    federal_candidate boolean,
    url character varying COLLATE pg_catalog."default",
    is_deleted boolean
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.pages
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.pages TO nyufbpolads;

-- Table: public.region_impressions

DROP TABLE IF EXISTS public.region_impressions;

CREATE TABLE public.region_impressions
(
    ad_archive_id bigint,
    region_id integer,
    min_impressions integer,
    min_spend integer,
    max_impressions integer,
    max_spend integer,
    crawl_date date,
    most_recent boolean,
    nyu_id bigint NOT NULL DEFAULT nextval('region_impressions_nyu_id1_seq'::regclass),
    CONSTRAINT region_impressions_unique_ad_archive_id UNIQUE (ad_archive_id, region_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.region_impressions
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.region_impressions TO nyufbpolads;

-- Index: region_impressions_archive_id_idx

DROP INDEX IF EXISTS public.region_impressions_archive_id_idx;

CREATE INDEX region_impressions_archive_id_idx
    ON public.region_impressions USING btree
    (ad_archive_id ASC NULLS LAST)
    TABLESPACE pg_default;

-- Table: public.regions

DROP TABLE IF EXISTS public.regions;

CREATE TABLE public.regions
(
    name character varying COLLATE pg_catalog."default",
    id integer NOT NULL DEFAULT nextval('regions_id_seq'::regclass),
    CONSTRAINT regions_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.regions
    OWNER to nyufbpolads;

GRANT ALL ON TABLE public.regions TO nyufbpolads;

