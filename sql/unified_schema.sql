-- These tables exist in DB fb_global_ads.

-- Uncomment these to drop tables. 
--  DROP TABLE IF EXISTS ad_countries cascade;
--  DROP TABLE IF EXISTS pages cascade;
--  DROP TABLE IF EXISTS impressions cascade;
--  DROP TABLE IF EXISTS funder_metadata cascade;
--  DROP TABLE IF EXISTS ad_metadata cascade;
--  DROP TABLE IF EXISTS page_metadata cascade;
--  DROP TABLE IF EXISTS ad_creatives cascade;
--  DROP TABLE IF EXISTS ads cascade;
--  DROP TABLE IF EXISTS demo_impressions cascade;
--  DROP TABLE IF EXISTS region_impressions cascade;
--  DROP TABLE IF EXISTS demo_impression_results cascade;
--  DROP TABLE IF EXISTS region_impression_results cascade;


CREATE TABLE pages (
  page_id bigint NOT NULL,
  page_name character varying NOT NULL,
  page_url character varying,
  PRIMARY KEY (page_id)
);
CREATE TABLE ads (
  archive_id bigint NOT NULL,
  ad_creation_time date,
  ad_delivery_start_time date,
  ad_delivery_stop_time date,
  page_id bigint,
  currency character varying (4),
  ad_snapshot_url character varying,
  funding_entity character varying,
  PRIMARY KEY (archive_id),
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE ad_countries(
  archive_id bigint,
  country_code character varying,
  PRIMARY KEY (archive_id, country_code),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE impressions (
  archive_id bigint,
  ad_status bigint,
  min_spend decimal(10, 2),
  max_spend decimal(10, 2),
  min_impressions integer,
  max_impressions integer,
  PRIMARY KEY (archive_id),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE funder_metadata (
  funder_id SERIAL,
  funder_name character varying,
  funder_type character varying,
  legal_entity_id character varying,
  legal_entity_name character varying,
  funder_country character varying,
  parent_id bigint,
  partisan_lean character varying,
  PRIMARY KEY (funder_id)
);
CREATE TABLE ad_metadata (
  archive_id bigint,
  funder_id bigint,
  category character varying,
  ad_id bigint,
  PRIMARY KEY (archive_id),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT funder_id_fk FOREIGN KEY (funder_id) REFERENCES funder_metadata (funder_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE page_metadata (
  page_id bigint NOT NULL,
  fb_category_list character varying,
  page_type character varying,
  country_code_list character varying,
  page_owner character varying,
  page_status character varying,
  PRIMARY KEY (page_id),
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE ad_creatives (
  ad_creative_id bigserial PRIMARY KEY,
  archive_id bigint NOT NULL,
  ad_creative_body character varying,
  ad_creative_link_url character varying,
  ad_creative_link_caption character varying,
  ad_creative_link_title character varying,
  ad_creative_link_description character varying,
  -- TODO(macpd): how to store/differentiate videos?
  snapshot_fetch_time timestamp,
  text_sim_hash character varying,
  text_sha256_hash character varying,
  image_downloaded_url character varying,
  image_bucket_path character varying,
  image_sim_hash character varying,
  image_sha256_hash character varying,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_creative_per_archive_id UNIQUE(archive_id, text_sha256_hash, image_sha256_hash)
);
CREATE TABLE demo_impressions (
  archive_id bigint,
  age_group character varying,
  gender character varying,
  spend_percentage decimal(5, 2),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT spend_is_percentage check (
    spend_percentage >= 0
    AND spend_percentage <= 100
  ),
  CONSTRAINT unique_demos_per_ad UNIQUE(archive_id, age_group, gender)
);
CREATE TABLE region_impressions (
  archive_id bigint,
  region character varying,
  spend_percentage decimal(5, 2),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT spend_is_percentage check (
    spend_percentage >= 0
    AND spend_percentage <= 100
  ),
  CONSTRAINT unique_regions_per_ad UNIQUE(archive_id, region)
);
CREATE TABLE demo_impression_results (
  archive_id bigint,
  age_group character varying,
  gender character varying,
  min_spend decimal(10, 2),
  max_spend decimal(10, 2),
  min_impressions integer,
  max_impressions integer,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_demo_results UNIQUE(archive_id, age_group, gender)
);
CREATE TABLE region_impression_results (
  archive_id bigint,
  region character varying,
  min_spend decimal(10, 2),
  max_spend decimal(10, 2),
  min_impressions integer,
  max_impressions integer,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_region_results UNIQUE(archive_id, region)
);
