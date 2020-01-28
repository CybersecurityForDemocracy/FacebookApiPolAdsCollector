--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.14
-- Dumped by pg_dump version 9.5.14

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: ad_categories; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.ad_categories (
    name character varying,
    id integer NOT NULL
);


ALTER TABLE public.ad_categories OWNER TO db_owner_name;

--
-- Name: ad_categories_id_seq; Type: SEQUENCE; Schema: public; Owner: db_owner_name
--

CREATE SEQUENCE public.ad_categories_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ad_categories_id_seq OWNER TO db_owner_name;

--
-- Name: ad_categories_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: db_owner_name
--

ALTER SEQUENCE public.ad_categories_id_seq OWNED BY public.ad_categories.id;


--
-- Name: ad_sponsors; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.ad_sponsors (
    id integer NOT NULL,
    name character varying(2000),
    federal_candidate boolean,
    nyu_category character varying,
    partisan_lean character varying,
    cmte_fec_id text,
    parent_ad_sponsor_id integer,
    cmte_ein text
);


ALTER TABLE public.ad_sponsors OWNER TO db_owner_name;

--
-- Name: ad_sponsor_id_seq; Type: SEQUENCE; Schema: public; Owner: db_owner_name
--

CREATE SEQUENCE public.ad_sponsor_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ad_sponsor_id_seq OWNER TO db_owner_name;

--
-- Name: ad_sponsor_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: db_owner_name
--

ALTER SEQUENCE public.ad_sponsor_id_seq OWNED BY public.ad_sponsors.id;


--
-- Name: ads; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.ads (
    text text,
    start_date timestamp(4) with time zone,
    end_date timestamp(4) with time zone,
    creation_date timestamp(4) with time zone,
    page_id bigint,
    currency character varying(255),
    snapshot_url character varying,
    is_active boolean,
    ad_sponsor_id integer,
    archive_id bigint NOT NULL,
    nyu_id bigint NOT NULL,
    link_caption character varying,
    link_description character varying,
    link_title character varying,
    ad_category_id integer,
    ad_id bigint,
    country_code character varying
);


ALTER TABLE public.ads OWNER TO db_owner_name;

--
-- Name: ads_nyu_id_seq; Type: SEQUENCE; Schema: public; Owner: db_owner_name
--

CREATE SEQUENCE public.ads_nyu_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.ads_nyu_id_seq OWNER TO db_owner_name;

--
-- Name: ads_nyu_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: db_owner_name
--

ALTER SEQUENCE public.ads_nyu_id_seq OWNED BY public.ads.nyu_id;


--
-- Name: demo_groups; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.demo_groups (
    age character varying(255),
    gender character varying(255),
    id integer NOT NULL
);


ALTER TABLE public.demo_groups OWNER TO db_owner_name;

--
-- Name: demo_groups_id_seq; Type: SEQUENCE; Schema: public; Owner: db_owner_name
--

CREATE SEQUENCE public.demo_groups_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.demo_groups_id_seq OWNER TO db_owner_name;

--
-- Name: demo_groups_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: db_owner_name
--

ALTER SEQUENCE public.demo_groups_id_seq OWNED BY public.demo_groups.id;


--
-- Name: demo_impressions; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.demo_impressions (
    ad_archive_id bigint,
    demo_id integer,
    min_impressions integer,
    max_impressions integer,
    min_spend integer,
    max_spend integer,
    crawl_date date,
    nyu_id bigint,
    most_recent boolean
);


ALTER TABLE public.demo_impressions OWNER TO db_owner_name;

--
-- Name: images; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.images (
    image_url character varying,
    image_phash character varying
);


ALTER TABLE public.images OWNER TO db_owner_name;

--
-- Name: impressions; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.impressions (
    ad_archive_id bigint,
    crawl_date date,
    min_impressions integer,
    min_spend integer,
    max_impressions integer,
    max_spend integer,
    nyu_id bigint,
    most_recent boolean
);


ALTER TABLE public.impressions OWNER TO db_owner_name;

--
-- Name: multiple_ad_categories; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.multiple_ad_categories (
    ad_archive_id bigint,
    ad_category_id integer
);


ALTER TABLE public.multiple_ad_categories OWNER TO db_owner_name;

--
-- Name: page_categories; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.page_categories (
    page_id bigint,
    category character varying
);


ALTER TABLE public.page_categories OWNER TO db_owner_name;

--
-- Name: pages; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.pages (
    page_name character varying(255),
    page_id bigint,
    federal_candidate boolean,
    url character varying,
    is_deleted boolean
);


ALTER TABLE public.pages OWNER TO db_owner_name;

--
-- Name: region_impressions; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.region_impressions (
    ad_archive_id bigint,
    region_id integer,
    min_impressions integer,
    min_spend integer,
    max_impressions integer,
    max_spend integer,
    crawl_date date,
    nyu_id bigint,
    most_recent boolean
);


ALTER TABLE public.region_impressions OWNER TO db_owner_name;

--
-- Name: regions; Type: TABLE; Schema: public; Owner: db_owner_name
--

CREATE TABLE public.regions (
    name character varying,
    id integer NOT NULL
);


ALTER TABLE public.regions OWNER TO db_owner_name;

--
-- Name: regions_id_seq; Type: SEQUENCE; Schema: public; Owner: db_owner_name
--

CREATE SEQUENCE public.regions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.regions_id_seq OWNER TO db_owner_name;

--
-- Name: regions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: db_owner_name
--

ALTER SEQUENCE public.regions_id_seq OWNED BY public.regions.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: db_owner_name
--

ALTER TABLE ONLY public.ad_categories ALTER COLUMN id SET DEFAULT nextval('public.ad_categories_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: db_owner_name
--

ALTER TABLE ONLY public.ad_sponsors ALTER COLUMN id SET DEFAULT nextval('public.ad_sponsor_id_seq'::regclass);


--
-- Name: nyu_id; Type: DEFAULT; Schema: public; Owner: db_owner_name
--

ALTER TABLE ONLY public.ads ALTER COLUMN nyu_id SET DEFAULT nextval('public.ads_nyu_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: db_owner_name
--

ALTER TABLE ONLY public.demo_groups ALTER COLUMN id SET DEFAULT nextval('public.demo_groups_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: db_owner_name
--

ALTER TABLE ONLY public.regions ALTER COLUMN id SET DEFAULT nextval('public.regions_id_seq'::regclass);


--
-- Name: ad_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: db_owner_name
--

ALTER TABLE ONLY public.ad_categories
    ADD CONSTRAINT ad_categories_pkey PRIMARY KEY (id);


--
-- Name: demo_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: db_owner_name
--

ALTER TABLE ONLY public.demo_groups
    ADD CONSTRAINT demo_groups_pkey PRIMARY KEY (id);


--
-- Name: regions_pkey; Type: CONSTRAINT; Schema: public; Owner: db_owner_name
--

ALTER TABLE ONLY public.regions
    ADD CONSTRAINT regions_pkey PRIMARY KEY (id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

