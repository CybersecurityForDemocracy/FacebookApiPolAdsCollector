CREATE TABLE pages (
  page_id bigint NOT NULL,
  page_name character varying NOT NULL,
  page_url character varying,
  PRIMARY KEY (page_id)
);
CREATE TABLE ads (
  archive_id bigint NOT NULL,
  ad_text character varying,
  creation_data date,
  end_data date,
  page_id bigint,
  currency character varying (4),
  link_caption character varying,
  link_title character varying,
  creative_url character varying,
  sponsor_label character varying,
  country_codes character varying,
  PRIMARY KEY (archive_id),
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE impressions (
  archive_id bigint,
  ad_status bigint,
  min_spend decimal(10, 2),
  max_spend decimal(10, 2),
  min_impressions integer,
  max_impressions integer,
  last_active date,
  PRIMARY KEY (archive_id),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT funder_id_fk FOREIGN KEY (funder_id) REFERENCES funder_metadata (funder_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE funder_metadata (
  funder_id bigint NOT NULL,
  funder_name character varying,
  funder_type character varying,
  legal_entity_id character varying,
  legal_entity_name character varying,
  funder_country character varying,
  parent_id bigint,
  partisan_lean character varying,
  PRIMARY KEY (funder_id),
);
CREATE TABLE ad_metadata (
  archive_id bigint,
  funder_id bigint,
  category character varying,
  ad_id bigint,
  text_hash character varying,
  last_active date,
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
CREATE TABLE ad_images (
  archive_id bigint,
  image_url character varying,
  sim_hash character varying,
  PRIMARY KEY (archive_id),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
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
  )
);
CREATE TABLE region_impressions (
  archive_id bigint,
  region character varying,
  spend_percentage decimal(5, 2),
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT spend_is_percentage check (
    spend_percentage >= 0
    AND spend_percentage <= 100
  )
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
);
CREATE TABLE region_impression_results (
  archive_id bigint,
  region character varying,
  min_spend decimal(10, 2),
  max_spend decimal(10, 2),
  min_impressions integer,
  max_impressions integer,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
);