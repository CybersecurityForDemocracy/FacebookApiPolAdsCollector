-- These tables exist in DB fb_global_ads.

-- Uncomment these to drop tables. 
--  DROP TABLE IF EXISTS ad_countries cascade;
--  DROP TABLE IF EXISTS pages cascade;
--  DROP TABLE IF EXISTS impressions cascade;
--  DROP TABLE IF EXISTS funder_metadata cascade;
--  DROP TABLE IF EXISTS ad_metadata cascade;
--  DROP TABLE IF EXISTS page_metadata cascade;
--  DROP TABLE IF EXISTS ad_snapshot_metadata cascade;
--  DROP TABLE IF EXISTS ad_creatives cascade;
--  DROP TABLE IF EXISTS ads cascade;
--  DROP TABLE IF EXISTS demo_impressions cascade;
--  DROP TABLE IF EXISTS region_impressions cascade;
--  DROP TABLE IF EXISTS demo_impression_results cascade;
--  DROP TABLE IF EXISTS region_impression_results cascade;


CREATE TABLE pages (
  page_id bigint NOT NULL,
  page_name character varying NOT NULL,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (page_id)
);
CREATE TABLE ads (
  archive_id bigint NOT NULL,
  ad_creative_body character varying,
  ad_creation_time date,
  ad_delivery_start_time date,
  ad_delivery_stop_time date,
  page_id bigint,
  currency character varying (4),
  ad_creative_link_caption character varying,
  ad_creative_link_title character varying,
  ad_creative_link_description character varying,
  ad_snapshot_url character varying,
  funding_entity character varying,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (archive_id),
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE ad_countries(
  archive_id bigint,
  country_code character varying,
  PRIMARY KEY (archive_id, country_code),
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE impressions (
  archive_id bigint,
  ad_status bigint,
  min_spend decimal(10, 2),
  max_spend decimal(10, 2),
  min_impressions integer,
  max_impressions integer,
  potential_reach_min bigint,
  potential_reach_max bigint,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
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
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (funder_id)
);
CREATE TABLE ad_metadata (
  archive_id bigint,
  funder_id bigint,
  ad_id bigint,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (archive_id),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT funder_id_fk FOREIGN KEY (funder_id) REFERENCES funder_metadata (funder_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE page_metadata (
  page_id bigint NOT NULL,
  page_url character varying,
  page_type character varying,
  country_code_list character varying,
  page_owner character varying,
  page_status character varying,
  advertiser_score decimal(8, 6),
  partisan_lean character varying,
  party character varying,
  fec_id character varying,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (page_id),
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE ad_snapshot_metadata (
  archive_id bigint NOT NULL,
  needs_scrape boolean DEFAULT TRUE,
  snapshot_fetch_time timestamp with timezone,
  snapshot_fetch_status int,
  snapshot_fetch_batch_id bigint,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (archive_id),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT batch_id_fk FOREIGN KEY (snapshot_fetch_batch_id) REFERENCES snapshot_fetch_batches (batch_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL
);
CREATE TABLE ad_creatives (
  ad_creative_id bigserial PRIMARY KEY,
  archive_id bigint NOT NULL,
  ad_creative_body character varying,
  ad_creative_body_language character varying,
  ad_creative_link_url character varying,
  ad_creative_link_caption character varying,
  ad_creative_link_title character varying,
  ad_creative_link_description character varying,
  -- TODO(macpd): how to store/differentiate videos?
  text_sim_hash character varying,
  text_sha256_hash character varying,
  image_downloaded_url character varying,
  image_bucket_path character varying,
  image_sim_hash character varying,
  image_sha256_hash character varying,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_creative_per_archive_id UNIQUE(archive_id, text_sha256_hash, image_sha256_hash)
);
CREATE TABLE demo_impressions (
  archive_id bigint,
  age_group character varying,
  gender character varying,
  spend_percentage decimal(5, 2),
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
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
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
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
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
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
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_region_results UNIQUE(archive_id, region)
);
CREATE TABLE snapshot_fetch_batches (
  batch_id bigserial PRIMARY KEY,
  time_started timestamp with time zone,
  time_completed timestamp with time zone,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
);

CREATE TABLE ad_ids (
  ad_id bigint PRIMARY KEY,
  archive_id bigint NOT NULL,
  CONSTRAINT unique_ad_id_archive_id UNIQUE(ad_id, archive_id)
);

-- Triggers to automatatically update the last_modified_time on every update.
CREATE EXTENSION IF NOT EXISTS moddatetime;

CREATE TRIGGER pages_moddatetime
BEFORE UPDATE ON pages
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ads_moddatetime
BEFORE UPDATE ON ads
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ad_countries_moddatetime
BEFORE UPDATE ON ad_countries
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER impressions_moddatetime
BEFORE UPDATE ON impressions
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER funder_metadata_moddatetime
BEFORE UPDATE ON funder_metadata
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ad_metadata_moddatetime
BEFORE UPDATE ON ad_metadata
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER page_metadata_moddatetime
BEFORE UPDATE ON page_metadata
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ad_snapshot_metadata_moddatetime
BEFORE UPDATE ON ad_snapshot_metadata
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ad_creatives_moddatetime
BEFORE UPDATE ON ad_creatives
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER demo_impressions_moddatetime
BEFORE UPDATE ON demo_impressions
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER region_impressions_moddatetime
BEFORE UPDATE ON region_impressions
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER demo_impression_results_moddatetime
BEFORE UPDATE ON demo_impression_results
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER region_impression_results_moddatetime
BEFORE UPDATE ON region_impression_results
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER snapshot_fetch_batches_moddatetime
BEFORE UPDATE ON snapshot_fetch_batches
FOR EACH ROW
EXECUTE PROCEDURE moddatetime(last_modified_time);
