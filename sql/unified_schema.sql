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
  page_url character varying,
  page_type character varying,
  country_code_list character varying,
  page_owner character varying,
  page_status character varying,
  advertiser_score decimal(8, 6),
  partisan_lean character varying,
  party character varying,
  fec_id character varying,
  PRIMARY KEY (page_id),
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE ad_snapshot_metadata (
  archive_id bigint NOT NULL,
  needs_scrape boolean DEFAULT TRUE,
  snapshot_fetch_time timestamp with timezone,
  snapshot_fetch_status int,
  snapshot_fetch_batch_id bigint,
  PRIMARY KEY (archive_id),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT batch_id_fk FOREIGN KEY (snapshot_fetch_batch_id) REFERENCES snapshot_fetch_batches (batch_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE SET NULL
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
CREATE TABLE ad_creative_body_recognized_entities_json (
  text_sha256_hash character varying PRIMARY KEY,
  named_entity_recognition_json jsonb NOT NULL
);
CREATE TABLE ad_clusters (
  archive_id bigint PRIMARY KEY,
  ad_cluster_id bigint NOT NULL,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_creative_per_cluster UNIQUE(archive_id, ad_cluster_id)
);
CREATE TABLE ad_cluster_metadata (
  ad_cluster_id bigint PRIMARY KEY,
  min_spend_sum decimal(12, 2),
  max_spend_sum decimal(12, 2),
  min_impressions_sum bigint,
  max_impressions_sum bigint,
  min_ad_creation_time date,
  max_ad_creation_time date,
  canonical_archive_id bigint,
  -- Number of archive_id in cluster
  cluster_size bigint,
  -- Number of unique pages associated with the ads in a cluster.
  num_pages bigint,
  CONSTRAINT archive_id_fk FOREIGN KEY (canonical_archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE ad_cluster_topics (
  ad_cluster_id bigint,
  topic_id bigint,
  CONSTRAINT ad_cluster_id_fk FOREIGN KEY (ad_cluster_id) REFERENCES ad_cluster_metadata (ad_cluster_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT topic_id_fk FOREIGN KEY (topic_id) REFERENCES topics (topic_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  PRIMARY KEY (ad_cluster_id, topic_id)
);
CREATE TABLE ad_cluster_pages (
  ad_cluster_id bigint,
  page_id bigint,
  CONSTRAINT ad_cluster_id_fk FOREIGN KEY (ad_cluster_id) REFERENCES ad_cluster_metadata (ad_cluster_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_ad_cluster_and_page_id UNIQUE(ad_cluster_id, page_id)
);
CREATE TABLE ad_cluster_demo_impression_results (
  ad_cluster_id bigint,
  age_group character varying,
  gender character varying,
  min_spend_sum decimal(10, 2),
  max_spend_sum decimal(10, 2),
  min_impressions_sum bigint,
  max_impressions_sum bigint,
  CONSTRAINT ad_cluster_id_fk FOREIGN KEY (ad_cluster_id) REFERENCES ad_cluster_metadata (ad_cluster_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_ad_cluster_demo_results UNIQUE(ad_cluster_id, age_group, gender)
);
CREATE TABLE ad_cluster_region_impression_results (
  ad_cluster_id bigint,
  region character varying,
  min_spend_sum decimal(10, 2),
  max_spend_sum decimal(10, 2),
  min_impressions_sum bigint,
  max_impressions_sum bigint,
  CONSTRAINT ad_cluster_id_fk FOREIGN KEY (ad_cluster_id) REFERENCES ad_cluster_metadata (ad_cluster_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_ad_cluster_region_results UNIQUE(ad_cluster_id, region)
);
CREATE TABLE ad_cluster_types (
  ad_cluster_id bigint,
  ad_type character varying,
  CONSTRAINT ad_cluster_id_fk FOREIGN KEY (ad_cluster_id) REFERENCES ad_cluster_metadata (ad_cluster_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_ad_cluster_id_per_ad_type UNIQUE(ad_cluster_id, ad_type)
);
CREATE TABLE ad_cluster_recognized_entities (
  ad_cluster_id bigint,
  entity_id bigint NOT NULL,
  CONSTRAINT ad_cluster_id_fk FOREIGN KEY (ad_cluster_id) REFERENCES ad_cluster_metadata (ad_cluster_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT entity_id_fk FOREIGN KEY (entity_id) REFERENCES recognized_entities (entity_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_ad_cluster_id_per_recoginized_entity UNIQUE(ad_cluster_id, entity_id)
);
CREATE TABLE recognized_entities (
  entity_id bigserial PRIMARY KEY,
  entity_name character varying NOT NULL,
  entity_type character varying NOT NULL,
  CONSTRAINT unique_name_and_type UNIQUE(entity_name, entity_type)
);
CREATE TABLE ad_creative_to_recognized_entities (
  ad_creative_id bigint NOT NULL,
  entity_id bigint NOT NULL,
  CONSTRAINT ad_creative_id_fk FOREIGN KEY (ad_creative_id) REFERENCES ad_creatives (ad_creative_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT entity_id_fk FOREIGN KEY (entity_id) REFERENCES recognized_entities (entity_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE snapshot_fetch_batches (
  batch_id bigserial PRIMARY KEY,
  time_started timestamp with time zone,
  time_completed timestamp with time zone
);
CREATE TABLE topics (
  topic_id bigserial PRIMARY_KEY,
  topic_name character varying NOT NULL UNIQUE
);
CREATE TABLE ad_topics (
  archive_id bigint NOT NULL,
  topic_id bigint NOT NULL,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT topic_id_fk FOREIGN KEY (topic_id) REFERENCES topics (topic_id) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE,
  PRIMARY KEY (archive_id, topic_id)
);


CREATE INDEX ad_clusters_ad_cluster_id_idx ON public.ad_clusters USING btree (ad_cluster_id);
CREATE INDEX ad_clusters_archive_id_idx ON public.ad_clusters USING btree (archive_id);
CREATE INDEX ad_cluster_metadata_min_ad_creation_time_idx ON public.ad_cluster_metadata USING btree (min_ad_creation_time);
CREATE INDEX ad_cluster_metadata_max_ad_creation_time_idx ON public.ad_cluster_metadata USING btree (max_ad_creation_time);
CREATE INDEX ad_cluster_pages_ad_cluster_id_idx ON public.ad_cluster_pages USING btree (ad_cluster_id);
CREATE INDEX ad_cluster_pages_page_id_idx ON public.ad_cluster_pages USING btree (page_id);
CREATE INDEX ad_topics_topic_id ON public.ad_topics USING btree (topic_id);
CREATE INDEX ad_cluster_topics_ad_cluster_id_idx ON public.ad_cluster_topics USING btree (ad_cluster_id);
CREATE INDEX ads_ads_creation_time_idx ON public.ads USING btree (ad_creation_time);
CREATE INDEX ads_page_id_idx ON public.ads USING btree (page_id);
