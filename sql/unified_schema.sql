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
  last_active_date date,
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
  -- Page owner is parent page_id
  page_owner bigint,
  page_status character varying,
  advertiser_score decimal(8, 6),
  partisan_lean character varying,
  party character varying,
  fec_id character varying,
  candidate_full_name character varying,
  candidate_last_name character varying,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  PRIMARY KEY (page_id),
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE page_name_history (
  page_id bigint NOT NULL,
  page_name character varying NOT NULL,
  last_seen timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT page_id_fk FOREIGN KEY (page_id) REFERENCES pages (page_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT unique_id_and_name UNIQUE(page_id, page_name)
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
  ad_creative_link_button_text character varying,
  -- TODO(macpd): how to store/differentiate videos?
  text_sim_hash character varying,
  text_sha256_hash character varying,
  image_downloaded_url character varying,
  image_bucket_path character varying,
  image_sim_hash character varying,
  image_sha256_hash character varying,
  video_downloaded_url character varying,
  video_bucket_path character varying,
  video_sha256_hash character varying,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT archive_id_fk FOREIGN KEY (archive_id) REFERENCES ads (archive_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  -- If this is changed make sure to add/remove fields from fb_ad_creative_retriever.AdCreativeRecordUniqueConstraintAttributes accordingly
  CONSTRAINT unique_creative_per_archive_id UNIQUE(archive_id, text_sha256_hash, image_sha256_hash, video_sha256_hash)
);
CREATE TABLE demo_impressions (
  archive_id bigint,
  age_group character varying,
  gender character varying,
  spend_percentage decimal(7, 6),
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
  spend_percentage decimal(7, 6),
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
  spend_estimate numeric(10,2),
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
  spend_estimate numeric(10,2),
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
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ads_moddatetime
BEFORE UPDATE ON ads
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ad_countries_moddatetime
BEFORE UPDATE ON ad_countries
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER impressions_moddatetime
BEFORE UPDATE ON impressions
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER funder_metadata_moddatetime
BEFORE UPDATE ON funder_metadata
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ad_metadata_moddatetime
BEFORE UPDATE ON ad_metadata
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER page_metadata_moddatetime
BEFORE UPDATE ON page_metadata
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER page_name_history_moddatetime
BEFORE UPDATE ON page_name_history
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ad_snapshot_metadata_moddatetime
BEFORE UPDATE ON ad_snapshot_metadata
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER ad_creatives_moddatetime
BEFORE UPDATE ON ad_creatives
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER demo_impressions_moddatetime
BEFORE UPDATE ON demo_impressions
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER region_impressions_moddatetime
BEFORE UPDATE ON region_impressions
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER demo_impression_results_moddatetime
BEFORE UPDATE ON demo_impression_results
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER region_impression_results_moddatetime
BEFORE UPDATE ON region_impression_results
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE TRIGGER snapshot_fetch_batches_moddatetime
BEFORE UPDATE ON snapshot_fetch_batches
FOR EACH ROW
WHEN (OLD IS DISTINCT FROM NEW)
EXECUTE PROCEDURE moddatetime(last_modified_time);

CREATE INDEX ads_page_id_idx ON public.ads USING btree (page_id);
CREATE INDEX ads_page_id_ad_delivery_start_time_idx ON public.ads USING btree (page_id, ad_delivery_start_time ASC);

-- Crowdtangle database

CREATE TABLE public.accounts (
  id bigint PRIMARY KEY,
  account_type character varying NOT NULL,
  handle character varying,
  name character varying NOT NULL,
  page_admin_top_country character varying,
  platform character varying NOT NULL,
  platform_id character varying,
  profile_image character varying,
  subscriber_count bigint,
  url character varying,
  verified boolean NOT NULL,
  updated timestamp with time zone NOT NULL,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE TABLE public.posts (
  id character varying PRIMARY KEY,
  account_id bigint NOT NULL,
  branded_content_sponsor_account_id bigint,
  message character varying,
  title character varying,
  platform character varying NOT NULL,
  platform_id character varying NOT NULL,
  post_url character varying NOT NULL,
  subscriber_count bigint,
  type character varying NOT NULL,
  updated timestamp with time zone NOT NULL,
  video_length_ms bigint,
  image_text character varying,
  legacy_id bigint,
  caption character varying,
  link character varying,
  date timestamp with time zone,
  description character varying,
  score double precision,
  live_video_status character varying,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT account_id_fk FOREIGN KEY (account_id) REFERENCES public.accounts (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT branded_content_sponsor_account_id_fk FOREIGN KEY (branded_content_sponsor_account_id) REFERENCES public.accounts (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE INDEX posts_post_id_updated_idx ON public.posts USING btree (id, updated);

CREATE TABLE public.post_statistics_actual (
  post_id character varying PRIMARY KEY,
  updated timestamp with time zone NOT NULL,
  angry_count bigint,
  comment_count bigint,
  favorite_count bigint,
  haha_count bigint,
  like_count bigint,
  love_count bigint,
  sad_count bigint,
  share_count bigint,
  up_count bigint,
  wow_count bigint,
  thankful_count bigint,
  care_count bigint,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT post_id_fk FOREIGN KEY (post_id) REFERENCES public.posts (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE public.post_statistics_expected (
  post_id character varying PRIMARY KEY,
  updated timestamp with time zone NOT NULL,
  angry_count bigint,
  comment_count bigint,
  favorite_count bigint,
  haha_count bigint,
  like_count bigint,
  love_count bigint,
  sad_count bigint,
  share_count bigint,
  up_count bigint,
  wow_count bigint,
  thankful_count bigint,
  care_count bigint,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT post_id_fk FOREIGN KEY (post_id) REFERENCES public.posts (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE public.expanded_links (
  post_id character varying NOT NULL,
  expanded character varying,
  original character varying,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT post_id_fk FOREIGN KEY (post_id) REFERENCES public.posts (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE public.media (
  post_id character varying NOT NULL,
  url_full character varying,
  url character varying,
  width bigint,
  height bigint,
  type character varying,
  last_modified_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
  CONSTRAINT post_id_fk FOREIGN KEY (post_id) REFERENCES public.posts (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);

CREATE TABLE public.dashboards (
  dashboard_id bigserial PRIMARY KEY,
  dashboard_name text NOT NULL
);

CREATE TABLE public.post_dashboards (
  post_id character varying NOT NULL,
  dashboard_id bigint NOT NULL,
  CONSTRAINT post_id_fk FOREIGN KEY (post_id) REFERENCES public.posts (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT dashboard_id_fk FOREIGN KEY (dashboard_id) REFERENCES public.dashboards (dashboard_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
  PRIMARY KEY(post_id, dashboard_id)
);

COMMENT ON COLUMN public.accounts.id IS 'The unique identifier of the account in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform on which the account exists.';
COMMENT ON COLUMN public.accounts.account_type IS 'For Facebook only. Options are facebook_page, facebook_profile, facebook_group.';
COMMENT ON COLUMN public.accounts.handle IS 'The handle or vanity URL of the account.';
COMMENT ON COLUMN public.accounts.name IS 'The name of the account.';
COMMENT ON COLUMN public.accounts.page_admin_top_country IS 'The ISO country code of the the country from where the plurality of page administrators operate.';
COMMENT ON COLUMN public.accounts.platform IS 'The platform on which the account exists. enum (facebook, instagram, reddit)';
COMMENT ON COLUMN public.accounts.platform_id IS 'The platform''s ID for the account. This is not shown for Facebook public users.';
COMMENT ON COLUMN public.accounts.profile_image IS 'A URL pointing at the profile image.';
COMMENT ON COLUMN public.accounts.subscriber_count IS 'The number of subscribers/likes/followers the account has. By default, the subscriberCount property will show page Followers (as of January 26, 2021). You can select either Page Likes or Followers in your Dashboard settings. https://help.crowdtangle.com/en/articles/4797890-faq-measuring-followers.';
COMMENT ON COLUMN public.accounts.url IS 'A link to the account on its platform.';
COMMENT ON COLUMN public.accounts.verified IS 'Whether or not the account is verified by the platform, if supported by the platform. If not supported, will return false.';

COMMENT ON COLUMN public.posts.id IS 'format ("account.id|postExternalId")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.';
COMMENT ON COLUMN public.posts.account_id IS 'See account https://github.com/CrowdTangle/API/wiki/Account';
COMMENT ON COLUMN public.posts.branded_content_sponsor_account_id IS 'See account https://github.com/CrowdTangle/API/wiki/Account . This field is only present for Facebook Page posts where there is a sponsoring Page.';
COMMENT ON COLUMN public.posts.message IS 'The user-submitted text on a post.';
COMMENT ON COLUMN public.posts.platform IS 'The platform on which the post was posted. E.g., Facebook, Instagram, etc. enum (facebook, instagram, reddit)';
COMMENT ON COLUMN public.posts.platform_id IS 'The platform''s ID for the post.';
COMMENT ON COLUMN public.posts.post_url IS 'The URL to access the post on its platform.';
COMMENT ON COLUMN public.posts.subscriber_count IS 'The number of subscriber the account had when the post was published. This is in contrast to the subscriberCount found on the account, which represents the current number of subscribers an account has.';
COMMENT ON COLUMN public.posts.type IS 'The type of the post. enum (album, igtv, link, live_video, live_video_complete, live_video_scheduled, native_video, photo, status, video, vine, youtube)';
COMMENT ON COLUMN public.posts.updated IS 'The date and time the post was most recently updated in CrowdTangle, which is most often via getting new scores from the platform. Time zone is UTC. "yyyy-mm-dd hh:mm:ss")';
COMMENT ON COLUMN public.posts.video_length_ms IS 'The length of the video in milliseconds.';
COMMENT ON COLUMN public.posts.image_text IS 'string  The text, if it exists, within an image.';
COMMENT ON COLUMN public.posts.legacy_id IS 'The legacy version of the unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.';
COMMENT ON COLUMN public.posts.caption IS 'The caption to a photo, if available.';
COMMENT ON COLUMN public.posts.link IS 'An external URL that the post links to, if available. (Facebook only)';
COMMENT ON COLUMN public.posts.date IS 'date ("yyyy‑mm‑dd hh:mm:ss")  The date and time the post was published. Time zone is UTC.';
COMMENT ON COLUMN public.posts.description IS 'Further details, if available. Associated with links or images across different platforms.';
COMMENT ON COLUMN public.posts.score IS 'The score of a post as measured by the request. E.g. it will represent the overperforming score if the request sortBy specifies overperforming, the interaction rate if the request specifies interaction_rate, etc.';
COMMENT ON COLUMN public.posts.live_video_status IS 'The status of the live video. ("live", "completed", "upcoming")';

COMMENT ON COLUMN public.post_statistics_actual.post_id IS 'format ("account.id|postExternalId")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.';
COMMENT ON COLUMN public.post_statistics_actual.angry_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_actual.comment_count IS 'Facebook, Instagram, Reddit';
COMMENT ON COLUMN public.post_statistics_actual.favorite_count IS 'Instagram';
COMMENT ON COLUMN public.post_statistics_actual.haha_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_actual.like_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_actual.love_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_actual.sad_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_actual.share_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_actual.up_count IS 'Reddit';
COMMENT ON COLUMN public.post_statistics_actual.wow_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_actual.thankful_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_actual.care_count IS 'Facebook';

COMMENT ON COLUMN public.post_statistics_expected.post_id IS 'format ("account.id|postExternalId")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.';
COMMENT ON COLUMN public.post_statistics_expected.angry_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_expected.comment_count IS 'Facebook, Instagram, Reddit';
COMMENT ON COLUMN public.post_statistics_expected.favorite_count IS 'Instagram';
COMMENT ON COLUMN public.post_statistics_expected.haha_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_expected.like_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_expected.love_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_expected.sad_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_expected.share_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_expected.up_count IS 'Reddit';
COMMENT ON COLUMN public.post_statistics_expected.wow_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_expected.thankful_count IS 'Facebook';
COMMENT ON COLUMN public.post_statistics_expected.care_count IS 'Facebook';

COMMENT ON COLUMN public.expanded_links.post_id IS 'format ("account.id|postExternalId")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.';
COMMENT ON COLUMN public.expanded_links.expanded IS 'Expanded version of original link';
COMMENT ON COLUMN public.expanded_links.original IS 'original link that came in the post (which are often shortened),';

COMMENT ON COLUMN public.media.post_id IS 'format ("account.id|postExternalId")   The unique identifier of the post in the CrowdTangle system. This ID is specific to CrowdTangle, not the platform from which the post originated.';
COMMENT ON COLUMN public.media.url_full IS 'The source of the full-sized version of the media. API returns this as |full| but that is a reserved word in postgres';
COMMENT ON COLUMN public.media.url IS 'The source of the media.';
COMMENT ON COLUMN public.media.width IS 'The width of the media.';
COMMENT ON COLUMN public.media.height IS 'The height of the media.';
COMMENT ON COLUMN public.media.type IS 'The type of the media. enum (photo or video)';

