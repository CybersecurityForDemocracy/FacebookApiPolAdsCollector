"""Encapsulation of database read, write, and update logic."""
from collections import defaultdict, namedtuple
import logging

import psycopg2
import psycopg2.extras

EntityRecord = namedtuple('EntityRecord', ['name', 'type'])
PageAgeAndMinImpressionSum = namedtuple('PageAgeAndMinImpressionSum',
                                        ['page_id', 'oldest_ad_date', 'min_impressions_sum'])
PageSnapshotFetchInfo = namedtuple('PageSnapshotFetchInfo',
                                   ['page_id', 'snapshot_fetch_status', 'count'])
PageRecord = namedtuple("PageRecord", ["id", "name"])

_DEFAULT_PAGE_SIZE = 250

class DBInterface():

    def __init__(self, connection):
        self.connection = connection

    def get_cursor(self, real_dict_cursor=False):
        if real_dict_cursor:
            return self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        return self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def existing_ads(self):
        cursor = self.get_cursor()
        existing_ad_query = "select archive_id, ad_delivery_stop_time from ads"
        cursor.execute(existing_ad_query)
        ads_to_end_time_map = dict()
        for row in cursor:
            ad_stop_time = row['ad_delivery_stop_time']
            ads_to_end_time_map[row['archive_id']] = ad_stop_time
        return ads_to_end_time_map

    def existing_archive_ids(self):
        cursor = self.get_cursor()
        existing_ad_query = "select archive_id from ads"
        cursor.execute(existing_ad_query)
        return {row['archive_id'] for row in cursor}

    def existing_pages(self):
        cursor = self.get_cursor()
        existing_pages_query = "select page_id from pages;"
        cursor.execute(existing_pages_query)
        existing_pages = {row['page_id'] for row in cursor}
        return existing_pages


    def page_records_to_max_last_seen(self):
        """Return dict of PageRecord -> max last_seen time for that PageRecord."""
        cursor = self.get_cursor()
        page_name_history_query = (
            "SELECT page_id, page_name, max(last_seen) as "
            "last_seen FROM page_name_history GROUP BY page_id, page_name;")
        cursor.execute(page_name_history_query)
        return {PageRecord(id=row['page_id'], name=row['page_name']): row['last_seen']
                for row in cursor}

    def existing_funding_entities(self):
        cursor = self.get_cursor()
        existing_funder_query = "select funder_id, funder_name from funder_metadata;"
        cursor.execute(existing_funder_query)
        existing_funders = dict()
        for row in cursor:
            existing_funders[row['funder_name']] = row['funder_id']
        return existing_funders

    def existing_ad_clusters(self):
        cursor = self.get_cursor()
        existing_ad_clusters_query = 'SELECT archive_id, ad_cluster_id FROM ad_clusters'
        cursor.execute(existing_ad_clusters_query)
        return {row['archive_id']: row['ad_cluster_id'] for row in cursor}

    def existing_recognized_entities(self):
        """Gets all regonized entities from DB as dict EntityRecord(name, type) -> entity_id."""
        cursor = self.get_cursor()
        existing_recognized_entities_query = (
            'SELECT entity_id, entity_name, entity_type FROM recognized_entities')
        cursor.execute(existing_recognized_entities_query)
        return {EntityRecord(name=row['entity_name'], type=row['entity_type']): row['entity_id']
                for row in cursor.fetchall()}

    def all_archive_ids_that_need_scrape(self):
        """Get ALL ad archive IDs marked as needs_scrape in ad_snapshot_metadata.

          Args:
          cursor: pyscopg2.Cursor DB cursor for query execution.
          Returns:
          list of archive IDs (str).
        """
        cursor = self.get_cursor()
        archive_ids_sample_query = cursor.mogrify(
            'SELECT archive_id from ad_snapshot_metadata '
            'WHERE needs_scrape = TRUE')
        cursor.execute(archive_ids_sample_query)
        results = cursor.fetchall()
        return [row['archive_id'] for row in results]

    def n_archive_ids_that_need_scrape(self, max_archive_ids=200):
        """Get N number of archive IDs marked as needs_scrape in ad_snapshot_metadata.
          Args:
          cursor: pyscopg2.Cursor DB cursor for query execution.
          max_archive_ids: int, limit on how many IDs to query DB for.
          Returns:
          list of archive IDs (str).
        """
        cursor = self.get_cursor()
        archive_ids_sample_query = cursor.mogrify(
            'SELECT archive_id from ad_snapshot_metadata '
            'WHERE needs_scrape = TRUE LIMIT %s;' % max_archive_ids)
        cursor.execute(archive_ids_sample_query)
        results = cursor.fetchall()
        return [row['archive_id'] for row in results]

    def all_ad_creative_image_simhashes(self):
        """Returns Dict image_sim_hash -> set of archive_ids.
        """
        cursor = self.get_cursor()
        duplicate_simhash_query = (
            'SELECT archive_id, image_sim_hash FROM ad_creatives WHERE image_sim_hash IS NOT NULL '
            'AND image_sim_hash != \'\' AND image_sim_hash NOT IN ('
            '\'00000000000000000000000000000000\', \'000000000000100c00000000000c001c\', '
            '\'000000000000000000000000000000ff\')'
        )
        cursor.execute(duplicate_simhash_query)
        sim_hash_to_archive_id_set = defaultdict(set)
        [sim_hash_to_archive_id_set[int(row['image_sim_hash'], 16)].add(row['archive_id']) for row in cursor.fetchall()]
        return sim_hash_to_archive_id_set

    def all_ad_creative_text_simhashes(self):
        """Returns Dict ad body text sim_hash -> set of archive_ids.
        """
        cursor = self.get_cursor()
        duplicate_simhash_query = (
            'SELECT archive_id, text_sim_hash FROM ad_creatives WHERE text_sim_hash IS NOT NULL '
            'AND text_sim_hash != \'\' AND length(ad_creative_body) > 9'
        )
        cursor.execute(duplicate_simhash_query)
        sim_hash_to_archive_id_set = defaultdict(set)
        [sim_hash_to_archive_id_set[int(row['text_sim_hash'], 16)].add(row['archive_id']) for row in cursor.fetchall()]
        return sim_hash_to_archive_id_set

    def duplicate_ad_creative_text_simhashes(self):
        """Returns list of ad creative text simhashes appearing 2 or more times.
        """
        cursor = self.get_cursor()
        duplicate_simhash_query = (
            'SELECT text_sim_hash FROM ad_creatives WHERE text_sim_hash IS NOT NULL GROUP BY '
            'text_sim_hash HAVING COUNT(*) > 1'
        )
        cursor.execute(duplicate_simhash_query)
        results = cursor.fetchall()
        return [row['text_sim_hash'] for row in results]

    def archive_ids_with_ad_creative_text_simhash(self, simhash):
        """Returns list of archive_id with specified body text simhash.

        Args:
            simhash: simhash (hex value as string (without leading 0x) to look
            for.
        Returns:
            list of archive_id (str) where ads_creatives.text_sim_hash matches
            specified simhash.
        """
        cursor = self.get_cursor()
        ids_with_simhash_query_template = (
            'SELECT archive_id FROM ad_creatives WHERE text_sim_hash = %s')
        cursor.execute(ids_with_simhash_query_template, (simhash,))
        return [row['archive_id'] for row in cursor.fetchall()]

    def ad_creative_ids_with_text_simhash(self, simhash):
        """Returns list of ad_creative_id with specified body text simhash.

        Args:
            simhash: simhash (hex value as string (without leading 0x) to look
            for.
        Returns:
            list of ad_creative_id (str) where ads_creatives.text_sim_hash
            matches specified simhash.
        """
        cursor = self.get_cursor()
        ids_with_simhash_query_template = (
            'SELECT ad_creative_id FROM ad_creatives WHERE text_sim_hash = %s')
        cursor.execute(ids_with_simhash_query_template, (simhash,))
        return [row['ad_creative_id'] for row in cursor.fetchall()]

    def get_stored_recognized_entities_for_text_sha256_hash(self, text_sha256_hash):
        cursor = self.get_cursor()
        query = ('SELECT named_entity_recognition_json FROM '
                 'ad_creative_body_recognized_entities_json WHERE text_sha256_hash = %s')
        cursor.execute(query, (text_sha256_hash,))
        result = cursor.fetchone()
        if result:
            return result['named_entity_recognition_json']
        return None

    def ad_creative_ids_with_text_sha256_hash(self, text_sha256_hash):
        cursor = self.get_cursor()
        query = ('SELECT ad_creative_id from ad_creatives WHERE text_sha256_hash = %s')
        cursor.execute(query, (text_sha256_hash,))
        return [row['ad_creative_id'] for row in cursor.fetchall()]

    def all_ads_with_nonempty_link_caption_or_body(self):
        """Generator yielding all ads that have nonempty ad_creative_link_caption or
        ad_creative_link_caption.

        Yields:
            rows of archive_id, ad_creative_body, ad_creative_link_caption
        """
        cursor = self.get_cursor()
        cursor.execute("SELECT archive_id, ad_creative_body, ad_creative_link_caption FROM ads "
                       "WHERE ad_creative_link_caption <> '' OR  ad_creative_body <> '';")
        for row in cursor:
            yield {'archive_id': row['archive_id'],
                   'ad_creative_body': row['ad_creative_body'],
                   'ad_creative_link_caption': row['ad_creative_link_caption']}


    def insert_funding_entities(self, new_funders):
        cursor = self.get_cursor()
        insert_funder_query = "INSERT INTO funder_metadata(funder_name) VALUES %s;"
        insert_template = "(%s)"
        psycopg2.extras.execute_values(cursor,
                                       insert_funder_query,
                                       new_funders,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def insert_pages(self, new_pages, new_page_name_history_records):
        cursor = self.get_cursor()
        insert_page_query = (
            "INSERT INTO pages(page_id, page_name) VALUES %s ON CONFLICT (page_id) DO NOTHING")
        insert_template = "(%(id)s, %(name)s)"
        new_page_list = [x._asdict() for x in new_pages]
        psycopg2.extras.execute_values(
            cursor,
            insert_page_query,
            new_page_list,
            template=insert_template,
            page_size=_DEFAULT_PAGE_SIZE)

        insert_page_metadata_query = (
            "INSERT INTO page_metadata(page_id, page_owner) VALUES %s "
            "on conflict (page_id) do nothing;")
        insert_page_metadata_template = "(%(id)s, %(id)s)"
        psycopg2.extras.execute_values(
            cursor, insert_page_metadata_query, new_page_list,
            template=insert_page_metadata_template, page_size=_DEFAULT_PAGE_SIZE)

        insert_page_name_history_query = (
            "INSERT INTO page_name_history (page_id, page_name, last_seen) VALUES %s "
            "ON CONFLICT (page_id, page_name) DO UPDATE SET last_seen = EXCLUDED.last_seen WHERE "
            "page_name_history.page_id = EXCLUDED.page_id AND "
            "page_name_history.page_name = EXCLUDED.page_name AND "
            "page_name_history.last_seen < EXCLUDED.last_seen;")
        insert_page_name_history_template = "(%(page_id)s, %(page_name)s, %(last_seen)s)"
        page_name_history_records_list = [
            {'page_id': k.id, 'page_name': k.name, 'last_seen': v} for k, v in
            new_page_name_history_records.items()]
        psycopg2.extras.execute_values(cursor,
                                       insert_page_name_history_query,
                                       page_name_history_records_list,
                                       template=insert_page_name_history_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def update_page_name_to_latest_seen(self):
        cursor = self.get_cursor()
        update_pages_page_name_to_latest = (
            '''UPDATE pages SET page_name = latest_page_names.page_name FROM (
                SELECT DISTINCT ON (page_id) page_id, page_name FROM page_name_history ORDER BY
                page_id, last_seen DESC) AS latest_page_names
            WHERE pages.page_id = latest_page_names.page_id AND
            pages.page_name != latest_page_names.page_name;''')
        cursor.execute(update_pages_page_name_to_latest)


    def insert_page_metadata(self, new_page_metadata):
        cursor = self.get_cursor()
        insert_page_metadata_query = (
            "INSERT INTO page_metadata(page_id, page_url, federal_candidate, page_owner) VALUES %s "
            "on conflict (page_id) do nothing;")
        insert_template = "(%(id)s, %(url)s, %(federal_candidate)s, %(id)s)"
        new_page_metadata_list = [x._asdict() for x in new_page_metadata]
        psycopg2.extras.execute_values(
            cursor, insert_page_metadata_query, new_page_metadata_list, template=insert_template,
            page_size=_DEFAULT_PAGE_SIZE)


    def insert_new_ads(self, new_ads):
        cursor = self.get_cursor()
        ad_insert_query = (
            "INSERT INTO ads(archive_id, ad_creative_body, ad_creation_time, "
            "ad_delivery_start_time, ad_delivery_stop_time, page_id, currency, "
            "ad_creative_link_caption, ad_creative_link_title, ad_creative_link_description, "
            "ad_snapshot_url, funding_entity) VALUES %s on conflict (archive_id) do nothing;"
        )
        insert_template = (
            "(%(archive_id)s, %(ad_creative_body)s, %(ad_creation_time)s, "
            "%(ad_delivery_start_time)s, %(ad_delivery_stop_time)s, %(page_id)s, %(currency)s, "
            "%(ad_creative_link_caption)s, %(ad_creative_link_title)s, "
            "%(ad_creative_link_description)s, %(ad_snapshot_url)s, %(funding_entity)s)"
        )
        new_ad_list = [x._asdict() for x in new_ads]
        psycopg2.extras.execute_values(cursor,
                                       ad_insert_query,
                                       new_ad_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

        ad_insert_query = (
            "INSERT INTO ad_countries(archive_id, country_code) VALUES %s on conflict (archive_id, "
            "country_code) do nothing;")
        insert_template = ("(%(archive_id)s, %(country_code)s)")
        psycopg2.extras.execute_values(cursor,
                                       ad_insert_query,
                                       new_ad_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

        # Mark newly found archive_id as needing scrape.
        snapshot_metadata_insert_query = (
            "INSERT INTO ad_snapshot_metadata (archive_id, needs_scrape) "
            "VALUES %s on conflict (archive_id) do nothing;")
        insert_template = ("(%(archive_id)s, TRUE)")
        psycopg2.extras.execute_values(cursor,
                                       snapshot_metadata_insert_query,
                                       new_ad_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def insert_new_impressions(self, new_impressions):
        cursor = self.get_cursor()
        # last_active_date is set to CURRENT_DATE in the insert values, but is not updated on
        # conflict so that it is only set to CURRENT_DATE for newly seen ads.
        impressions_insert_query = (
            "INSERT INTO impressions(archive_id, ad_status, min_spend, max_spend, min_impressions, "
            "max_impressions, potential_reach_min, potential_reach_max, last_active_date) "
            "VALUES %s "
            "on conflict (archive_id) do update set ad_status = EXCLUDED.ad_status, "
            "min_spend = EXCLUDED.min_spend, max_spend = EXCLUDED.max_spend, "
            "min_impressions = EXCLUDED.min_impressions, "
            "max_impressions = EXCLUDED.max_impressions, "
            "potential_reach_min = EXCLUDED.potential_reach_min, "
            "potential_reach_max = EXCLUDED.potential_reach_max;")

        insert_template = (
            "(%(archive_id)s, %(ad_status)s , %(spend__lower_bound)s, %(spend__upper_bound)s, "
            "%(impressions__lower_bound)s , %(impressions__upper_bound)s, "
            "%(potential_reach__lower_bound)s, %(potential_reach__upper_bound)s, CURRENT_DATE)")
        new_impressions_list = ([
            impression._asdict() for impression in new_impressions
        ])

        psycopg2.extras.execute_values(cursor,
                                       impressions_insert_query,
                                       new_impressions_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def insert_new_impression_demos(self, new_ad_demo_impressions):
        demo_impressions_list = ([
            impression._asdict() for impression in new_ad_demo_impressions
        ])
        cursor = self.get_cursor()
        impression_demo_insert_query = (
            "INSERT INTO demo_impressions(archive_id, age_group, gender, spend_percentage) "
            "VALUES %s on conflict on constraint unique_demos_per_ad do update set "
            "spend_percentage = EXCLUDED.spend_percentage;")
        insert_template = (
            '(%(archive_id)s, %(age_range)s, %(gender)s, %(spend_percentage)s)')

        psycopg2.extras.execute_values(cursor,
                                       impression_demo_insert_query,
                                       demo_impressions_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

        impression_demo_result_insert_query = (
            "INSERT INTO demo_impression_results(archive_id, age_group, gender, min_impressions, "
            "min_spend, max_impressions, max_spend) "
            "VALUES %s on conflict on constraint unique_demo_results do update "
            "set min_impressions = EXCLUDED.min_impressions, "
            "min_spend = EXCLUDED.min_spend, max_impressions = EXCLUDED.max_impressions, "
            "max_spend = EXCLUDED.max_spend;")
        insert_template = (
            "(%(archive_id)s, %(age_range)s, %(gender)s, %(min_impressions)s, %(min_spend)s, "
            "%(max_impressions)s , %(max_spend)s)")
        psycopg2.extras.execute_values(cursor,
                                       impression_demo_result_insert_query,
                                       demo_impressions_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def insert_new_impression_regions(self, new_ad_region_impressions):
        region_impressions_list = ([
            impression._asdict() for impression in new_ad_region_impressions
        ])
        cursor = self.get_cursor()
        impression_region_insert_query = (
            "INSERT INTO region_impressions(archive_id, region, spend_percentage) "
            "VALUES %s on conflict on constraint unique_regions_per_ad "
            "do update set spend_percentage = EXCLUDED.spend_percentage;")
        insert_template = ("(%(archive_id)s, %(region)s, %(spend_percentage)s)")
        psycopg2.extras.execute_values(cursor,
                                       impression_region_insert_query,
                                       region_impressions_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)
        impression_region_insert_query = (
            "INSERT INTO region_impression_results(archive_id, region, min_impressions, min_spend, "
            "max_impressions, max_spend) VALUES %s on conflict on constraint unique_region_results "
            "do update set min_impressions = EXCLUDED.min_impressions, "
            "min_spend = EXCLUDED.min_spend, max_impressions = EXCLUDED.max_impressions, "
            "max_spend = EXCLUDED.max_spend;")
        insert_template = (
            "(%(archive_id)s, %(region)s, %(min_impressions)s, %(min_spend)s, %(max_impressions)s, "
            "%(max_spend)s)")
        psycopg2.extras.execute_values(cursor,
                                       impression_region_insert_query,
                                       region_impressions_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def update_ad_snapshot_metadata(self, ad_snapshot_metadata_records):
        cursor = self.get_cursor()
        ad_snapshot_metadata_record_list = [x._asdict() for x in ad_snapshot_metadata_records]

        # Update ad_snapshot_metadata.needs_scrape to False now that ad_creatives have been scraped
        # and stored.
        update_query = (
            'UPDATE ad_snapshot_metadata SET snapshot_fetch_time = %(snapshot_fetch_time)s, '
            'needs_scrape = FALSE, snapshot_fetch_status = %(snapshot_fetch_status)s '
            'WHERE archive_id = %(archive_id)s')
        psycopg2.extras.execute_batch(cursor,
                                      update_query,
                                      ad_snapshot_metadata_record_list,
                                      page_size=_DEFAULT_PAGE_SIZE)


    def insert_ad_creative_records(self, ad_creative_records):
        cursor = self.get_cursor()
        insert_query = (
            'INSERT INTO ad_creatives(archive_id, ad_creative_body, ad_creative_body_language, '
            'ad_creative_link_url, ad_creative_link_title, ad_creative_link_caption, '
            'ad_creative_link_description, ad_creative_link_button_text, text_sha256_hash, '
            'text_sim_hash, image_downloaded_url, image_bucket_path, image_sim_hash, '
            'image_sha256_hash, video_downloaded_url, video_bucket_path, video_sha256_hash) '
            'VALUES %s ON CONFLICT '
            '(archive_id, text_sha256_hash, image_sha256_hash, video_sha256_hash) DO UPDATE SET '
            'ad_creative_body = EXCLUDED.ad_creative_body, '
            'ad_creative_body_language = EXCLUDED.ad_creative_body_language, '
            'ad_creative_link_url = EXCLUDED.ad_creative_link_url, '
            'ad_creative_link_title = EXCLUDED.ad_creative_link_title, '
            'ad_creative_link_caption = EXCLUDED.ad_creative_link_caption, '
            'ad_creative_link_description = EXCLUDED.ad_creative_link_description, '
            'ad_creative_link_button_text = EXCLUDED.ad_creative_link_button_text, '
            'text_sim_hash = EXCLUDED.text_sim_hash, '
            'image_downloaded_url = EXCLUDED.image_downloaded_url, '
            'image_bucket_path = EXCLUDED.image_bucket_path, '
            'image_sim_hash = EXCLUDED.image_sim_hash, '
            'video_downloaded_url = EXCLUDED.video_downloaded_url, '
            'video_bucket_path = EXCLUDED.video_bucket_path '
            'WHERE ad_creatives.archive_id = EXCLUDED.archive_id AND '
            'ad_creatives.text_sha256_hash = EXCLUDED.text_sha256_hash AND '
            'ad_creatives.image_sha256_hash = EXCLUDED.image_sha256_hash AND '
            # This updates creatives where video might previous have been missing
            '((ad_creatives.video_sha256_hash IS NULL AND EXCLUDED.video_sha256_hash IS NOT NULL) '
            'OR ad_creatives.video_sha256_hash = EXCLUDED.video_sha256_hash)')
        insert_template = (
            '(%(archive_id)s, %(ad_creative_body)s, %(ad_creative_body_language)s, '
            '%(ad_creative_link_url)s, %(ad_creative_link_title)s, '
            '%(ad_creative_link_caption)s, %(ad_creative_link_description)s, '
            '%(ad_creative_link_button_text)s, %(text_sha256_hash)s, '
            '%(text_sim_hash)s, %(image_downloaded_url)s, %(image_bucket_path)s, '
            '%(image_sim_hash)s, %(image_sha256_hash)s, %(video_downloaded_url)s, '
            '%(video_bucket_path)s, %(video_sha256_hash)s)')
        ad_creative_record_list = [x._asdict() for x in ad_creative_records]
        psycopg2.extras.execute_values(cursor,
                                       insert_query,
                                       ad_creative_record_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def insert_or_update_ad_cluster_records(self, ad_cluster_records):
        cursor = self.get_cursor()
        insert_query = (
            'INSERT INTO ad_clusters (archive_id, ad_cluster_id) VALUES %s ON CONFLICT '
            '(archive_id) DO UPDATE SET ad_cluster_id = EXCLUDED.ad_cluster_id')
        insert_template = '(%(archive_id)s, %(ad_cluster_id)s)'
        ad_cluster_record_list = [x._asdict() for x in ad_cluster_records]
        psycopg2.extras.execute_values(cursor,
                                       insert_query,
                                       ad_cluster_record_list,
                                       template=insert_template,
                                       page_size=10000)

    def update_ad_cluster_metadata(self):
        """Update min/max spend and impressions sums for each ad_cluster_id in
        ad_cluster_metadata."""
        cursor = self.get_cursor()
        ad_cluster_metadata_table_update_query = (
            'INSERT INTO ad_cluster_metadata (ad_cluster_id, min_spend_sum, max_spend_sum, '
            'min_impressions_sum, max_impressions_sum, min_ad_creation_time, max_ad_creation_time, '
            'canonical_archive_id, cluster_size) '
            '  (SELECT ad_cluster_id, SUM(min_spend) AS cluster_min_spend, '
            '  SUM(max_spend) AS cluster_max_spend, '
            '  SUM(min_impressions) AS cluster_min_impressions, '
            '  SUM(max_impressions) AS cluster_max_impressions, MIN(ad_creation_time) AS '
            '  min_ad_creation_time, MAX(ad_creation_time) AS max_ad_creation_time, '
            '  MIN(archive_id) AS canonical_archive_id, COUNT(archive_id) as cluster_size FROM '
            '  ad_clusters JOIN impressions USING(archive_id) JOIN ads USING(archive_id) '
            '  GROUP BY ad_cluster_id) '
            'ON CONFLICT (ad_cluster_id) DO UPDATE SET min_spend_sum = EXCLUDED.min_spend_sum, '
            'max_spend_sum = EXCLUDED.max_spend_sum, min_impressions_sum = '
            'EXCLUDED.min_impressions_sum, max_impressions_sum = EXCLUDED.max_impressions_sum, '
            'min_ad_creation_time = EXCLUDED.min_ad_creation_time, max_ad_creation_time = '
            'EXCLUDED.max_ad_creation_time, canonical_archive_id = EXCLUDED.canonical_archive_id, '
            'cluster_size = EXCLUDED.cluster_size')
        cursor.execute(ad_cluster_metadata_table_update_query)
        # Re-assign canonical archive_ids where we have crawl info for the archive IDs in that
        # cluster, and know they were crawled successfully.
        ad_cluster_canonical_id_update_query = (
            'UPDATE ad_cluster_metadata SET canonical_archive_id = data.canonical_archive_id FROM '
            '  (SELECT ad_cluster_id, MIN(archive_id) AS canonical_archive_id FROM ad_clusters '
            '   JOIN ad_snapshot_metadata USING(archive_id) WHERE snapshot_fetch_status = 1 '
            '   GROUP BY ad_cluster_id) as data '
            'WHERE ad_cluster_metadata.ad_cluster_id = data.ad_cluster_id')
        cursor.execute(ad_cluster_canonical_id_update_query)
        ad_cluster_demo_impression_results_update_query = (
            'INSERT INTO ad_cluster_demo_impression_results (ad_cluster_id, age_group, gender, '
            'min_spend_sum, max_spend_sum, min_impressions_sum, max_impressions_sum) ('
            '  SELECT ad_cluster_id, age_group, gender, SUM(min_spend), SUM(max_spend), '
            '  SUM(min_impressions), sum(max_impressions) FROM ad_clusters JOIN '
            '  demo_impression_results USING(archive_id) GROUP BY ad_cluster_id, age_group, gender'
            ') ON CONFLICT (ad_cluster_id, age_group, gender) DO UPDATE SET '
            'min_spend_sum = EXCLUDED.min_spend_sum, max_spend_sum = EXCLUDED.max_spend_sum, '
            'min_impressions_sum = EXCLUDED.min_impressions_sum, '
            'max_impressions_sum = EXCLUDED.max_impressions_sum')
        cursor.execute(ad_cluster_demo_impression_results_update_query)
        ad_cluster_region_impression_results_update_query = (
            'INSERT INTO ad_cluster_region_impression_results (ad_cluster_id, region, '
            'min_spend_sum, max_spend_sum, min_impressions_sum, max_impressions_sum) ('
            '  SELECT ad_cluster_id, region, SUM(min_spend), SUM(max_spend), SUM(min_impressions), '
            '   sum(max_impressions) FROM ad_clusters JOIN region_impression_results '
            '  USING(archive_id) GROUP BY ad_cluster_id, region'
            ') ON CONFLICT (ad_cluster_id, region) DO UPDATE SET min_spend_sum = '
            'EXCLUDED.min_spend_sum, max_spend_sum = EXCLUDED.max_spend_sum, min_impressions_sum = '
            'EXCLUDED.min_impressions_sum, max_impressions_sum = EXCLUDED.max_impressions_sum')
        cursor.execute(ad_cluster_region_impression_results_update_query)

        # Truncate table before repopulating to prevent stale mapping of cluster ID -> entity ID
        truncate_ad_cluster_recognized_entities_query = (
            'TRUNCATE TABLE ad_cluster_recognized_entities')
        ad_cluster_recognized_entities_update_query = (
            'INSERT INTO ad_cluster_recognized_entities (ad_cluster_id, entity_id) ('
            '  SELECT ad_cluster_id, entity_id FROM ad_clusters JOIN ad_creatives '
            '  USING(archive_id) JOIN ad_creative_to_recognized_entities USING(ad_creative_id) '
            '  GROUP BY ad_cluster_id, entity_id) '
            'ON CONFLICT (ad_cluster_id, entity_id) DO NOTHING')
        cursor.execute(truncate_ad_cluster_recognized_entities_query)
        cursor.execute(ad_cluster_recognized_entities_update_query)

        # Truncate table before repopulating to prevent stale mapping of cluster ID -> type
        truncate_ad_cluster_categories_query = 'TRUNCATE TABLE ad_cluster_types'
        ad_cluster_categories_update_query = (
            'INSERT INTO ad_cluster_types (ad_cluster_id, ad_type) ('
            '  SELECT ad_cluster_id, ad_type FROM ad_clusters JOIN ad_metadata USING(archive_id) '
            '  WHERE ad_type IS NOT NULL and ad_type != \'\' GROUP BY ad_cluster_id, ad_type) '
            'ON CONFLICT (ad_cluster_id, ad_type) DO NOTHING')
        cursor.execute(truncate_ad_cluster_categories_query)
        cursor.execute(ad_cluster_categories_update_query)

        # Truncate table before repopulating to prevent stale mapping of cluster ID -> page IDs
        truncate_ad_cluster_pages_query = 'TRUNCATE TABLE ad_cluster_pages'
        ad_cluster_pages_update_query = (
            'INSERT INTO ad_cluster_pages (ad_cluster_id, page_id) ('
            '  SELECT ad_cluster_id, page_id FROM pages JOIN ads USING(page_id) JOIN ad_clusters '
            '  USING(archive_id) GROUP BY ad_cluster_id, page_id)'
            'ON CONFLICT (ad_cluster_id, page_id) DO NOTHING')
        ad_cluster_metadata_page_count_update_query = (
            'UPDATE ad_cluster_metadata SET num_pages = data.num_pages FROM ( '
            '  SELECT ad_cluster_id, COUNT(DISTINCT(page_id)) AS num_pages FROM ad_cluster_pages '
            '  GROUP BY ad_cluster_id) AS data '
            'WHERE ad_cluster_metadata.ad_cluster_id = data.ad_cluster_id')
        cursor.execute(truncate_ad_cluster_pages_query)
        cursor.execute(ad_cluster_pages_update_query)
        cursor.execute(ad_cluster_metadata_page_count_update_query)


    def repopulate_ad_cluster_topic_table(self):
        cursor = self.get_cursor()
        truncate_query = ('TRUNCATE ad_cluster_topics')
        query = ('INSERT INTO ad_cluster_topics (ad_cluster_id, topic_id) (SELECT ad_cluster_id, '
                 'topic_id FROM ad_clusters JOIN ad_topics USING(archive_id)) ON CONFLICT '
                 '(ad_cluster_id, topic_id) DO NOTHING')
        cursor.execute(truncate_query)
        cursor.execute(query)


    def insert_named_entity_recognition_results(
            self, text_sha256_hash, named_entity_recognition_json):
        cursor = self.get_cursor()
        insert_query = (
            'INSERT INTO ad_creative_body_recognized_entities_json (text_sha256_hash, '
            'named_entity_recognition_json) VALUES (%(text_sha256_hash)s, '
            '%(named_entity_recognition_json)s) ON CONFLICT (text_sha256_hash) DO UPDATE SET '
            'named_entity_recognition_json = EXCLUDED.named_entity_recognition_json')
        cursor.execute(insert_query, ({'text_sha256_hash': text_sha256_hash,
                                       'named_entity_recognition_json':
                                       psycopg2.extras.Json(named_entity_recognition_json)}))

    def insert_recognized_entities(self, entity_records):
        cursor = self.get_cursor()
        insert_query = (
            'INSERT INTO recognized_entities(entity_name, entity_type) VALUES %s '
            'ON CONFLICT DO NOTHING')
        insert_template = '(%(name)s, %(type)s)'
        entity_record_list = [x._asdict() for x in entity_records]
        psycopg2.extras.execute_values(cursor,
                                       insert_query,
                                       entity_record_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def insert_ad_recognized_entity_records(self, ad_creative_to_recognized_entities_records):
        cursor = self.get_cursor()
        insert_query = (
            'INSERT INTO ad_creative_to_recognized_entities(ad_creative_id, entity_id) VALUES %s '
            'ON CONFLICT DO NOTHING')
        insert_template = '(%(ad_creative_id)s, %(entity_id)s)'
        entity_record_list = [x._asdict() for x in ad_creative_to_recognized_entities_records]
        psycopg2.extras.execute_values(cursor,
                                       insert_query,
                                       entity_record_list,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def make_snapshot_fetch_batches(self, batch_size=1000):
        """
        Add snapshots that need to be fetched into snapshot_fetch_batches in batches of batch_size.

        Args:
            batch_size: int size of batches to create.
        """
        logging.info('About to make batches (size %d) of unfetched archive IDs.', batch_size)
        read_cursor = self.get_cursor()
        read_cursor.arraysize = batch_size
        # Get all archive IDs that are unfetched and not part of an existing batch. Archive IDs are
        # ordered oldest to newest ad_creation time so that larger batch_id roughly corresponds to
        # newer ads.
        unbatched_archive_ids_query = (
            'SELECT ad_snapshot_metadata.archive_id FROM ad_snapshot_metadata '
            'JOIN ads ON ads.archive_id = ad_snapshot_metadata.archive_id WHERE '
            'ad_snapshot_metadata.needs_scrape = true AND '
            'ad_snapshot_metadata.snapshot_fetch_batch_id IS NULL ORDER BY ad_creation_time ASC')
        # This query inserts an empty row and gets back the autoincremented new batch_id.
        get_new_batch_id_query = (
            'INSERT INTO snapshot_fetch_batches (time_started, time_completed) VALUES (NULL, NULL) '
            'RETURNING batch_id')
        assign_batch_id_query = (
            'UPDATE ad_snapshot_metadata SET snapshot_fetch_batch_id = data.batch_id FROM '
            '(VALUES %s) AS data (batch_id, archive_id) WHERE '
            'ad_snapshot_metadata.archive_id = data.archive_id')
        assign_batch_id_template = '(%s, %s)'
        write_cursor = self.get_cursor()
        logging.info('Getting unfetched archive IDs.')
        read_cursor.execute(unbatched_archive_ids_query)
        fetched_rows = read_cursor.fetchmany()
        num_new_batches = 0
        while fetched_rows:
            write_cursor.execute(get_new_batch_id_query)
            batch_id = write_cursor.fetchone()['batch_id']
            assign_batch_id_args = [(int(batch_id), int(row['archive_id'])) for row in fetched_rows]
            logging.info(
                'Assigning batch ID %d to %d archive IDs.', batch_id, len(assign_batch_id_args))
            psycopg2.extras.execute_values(write_cursor,
                                           assign_batch_id_query,
                                           assign_batch_id_args,
                                           template=assign_batch_id_template,
                                           page_size=batch_size)
            num_new_batches += 1
            fetched_rows = read_cursor.fetchmany()

        logging.info('Added %d new batches.', num_new_batches)

    def get_archive_id_batch_to_fetch(self):
        cursor = self.get_cursor()
        # Get largest batch_id that has not yet been started (or was started more than 3 days ago
        # (which we assume is due to the task that claimed the batch dying before marking it
        # complete)).
        claim_batch_for_fetch_query = (
            'UPDATE snapshot_fetch_batches SET time_started = CURRENT_TIMESTAMP WHERE batch_id = '
            '(SELECT max(batch_id) FROM snapshot_fetch_batches WHERE time_completed IS NULL AND '
            '(time_started IS NULL OR time_started < CURRENT_TIMESTAMP - interval \'3 days\')) '
            'RETURNING batch_id')
        cursor.execute(claim_batch_for_fetch_query)
        row = cursor.fetchone()
        # COMMIT transaction to ensure no one else tries to take the same batch
        self.connection.commit()
        if not row:
            return None
        batch_id = row['batch_id']

        archive_id_batch_query = (
            'SELECT archive_id FROM ad_snapshot_metadata WHERE snapshot_fetch_batch_id = %s AND '
            'needs_scrape = TRUE')
        cursor.execute(archive_id_batch_query, (batch_id,))
        archive_ids_batch = [row['archive_id'] for row in cursor.fetchall()]
        # TODO(macpd): return this as a namedtuple
        return {'batch_id': batch_id, 'archive_ids': archive_ids_batch}

    def mark_fetch_batch_completed(self, batch_id):
        cursor = self.get_cursor()
        cursor.execute('UPDATE snapshot_fetch_batches SET time_completed = CURRENT_TIMESTAMP WHERE '
                       'batch_id = %s', (batch_id,))

    def release_uncompleted_fetch_batch(self, batch_id):
        cursor = self.get_cursor()
        cursor.execute(
            'UPDATE snapshot_fetch_batches SET time_started = NULL, time_completed = NULL WHERE '
            'time_completed IS NULL AND batch_id = %s', (batch_id,))


    def advertisers_age_and_sum_min_impressions(self, min_ad_creation_time):
        """Get age of pages with an ad created on or after the specified time."""
        advertiser_age_query = (
            'SELECT page_id, min(ad_creation_time) AS oldest_ad_date, '
            'sum(min_impressions) as min_impressions FROM ads '
            '  JOIN impressions ON ads.archive_id = impressions.archive_id '
            '  JOIN ad_creatives ON ads.archive_id = ad_creatives.archive_id '
            'WHERE ad_creatives.archive_id IS NOT NULL AND page_id IN '
            '(SELECT DISTINCT(page_id) FROM ads WHERE ad_creation_time >= '
            '%(min_ad_creation_time)s) GROUP BY page_id')
        cursor = self.get_cursor()
        cursor.execute(advertiser_age_query, {'min_ad_creation_time': min_ad_creation_time})
        page_details = []
        for row in cursor:
            page_details.append(PageAgeAndMinImpressionSum(
                page_id=row['page_id'],
                oldest_ad_date=row['oldest_ad_date'],
                min_impressions_sum=row['min_impressions']))
        return page_details

    def page_snapshot_status_fetch_counts(self, min_ad_creation_time):
        query = (
            'SELECT page_id, snapshot_fetch_status, COUNT(*) FROM ad_snapshot_metadata '
            'JOIN ads ON ad_snapshot_metadata.archive_id = ads.archive_id WHERE '
            'ads.archive_id IN ('
            '  SELECT archive_id FROM ads WHERE ad_creation_time > %(min_ad_creation_time)s) '
            'group by page_id, snapshot_fetch_status')
        cursor = self.get_cursor()
        cursor.execute(query, {'min_ad_creation_time': min_ad_creation_time})
        return [PageSnapshotFetchInfo(page_id=row['page_id'],
                                      snapshot_fetch_status=row['snapshot_fetch_status'],
                                      count=row['count']) for row in cursor.fetchall()]


    def unique_ad_body_texts(self, country, start_time, end_time):
        """ Return all unique ad_creative_body_text (and it's sha256) for ads active/started in a
        certain timeframe."""
        # TODO(macpd): handle end_time being none, or NULL in DB
        query = (
            'SELECT DISTINCT text_sha256_hash, ad_creatives.ad_creative_body FROM ad_creatives '
            '    JOIN ads ON ad_creatives.archive_id = ads.archive_id '
            '    JOIN ad_countries ON ads.archive_id = ad_countries.archive_id '
            'WHERE (ad_countries.country_code = %(country_upper)s OR '
            'ad_countries.country_code = %(country_lower)s) AND '
            'ads.ad_delivery_start_time >=  %(start_time)s AND '
            'ads.ad_delivery_stop_time <= %(end_time)s AND text_sha256_hash IS NOT NULL AND '
            'ad_creatives.ad_creative_body IS NOT NULL')
        cursor = self.get_cursor()
        cursor.execute(query, {'country_upper': country.upper(), 'country_lower': country.lower(),
                               'start_time': start_time, 'end_time': end_time})
        return {row['text_sha256_hash']: row['ad_creative_body'] for row in cursor.fetchall()}


    def ad_body_texts(self, start_time):
        """Get all ad creative body texts for given params. if start_time is false no time limit are
        applied (all ads from country).

        Args:
            start_time: datetime.date, datetime.datetime, str of earliest ad_delivery_start_time to
                inlude in results.
        Returns:
            list of tuples of (archive_id, and ad_creative_body).
        """
        if start_time:
            query = ('SELECT ads.archive_id, ads.ad_creative_body FROM ads '
                     '    JOIN ad_countries ON ads.archive_id = ad_countries.archive_id '
                     'WHERE ads.ad_delivery_start_time >=  %(start_time)s AND '
                     'ads.ad_creative_body IS NOT NULL')
            query_params = {'start_time': start_time}
        else:
            query = ('SELECT ads.archive_id, ads.ad_creative_body FROM ads '
                     '    JOIN ad_countries ON ads.archive_id = ad_countries.archive_id '
                     'WHERE ads.ad_creative_body IS NOT NULL')
            query_params = {}
        cursor = self.get_cursor()
        cursor.execute(query, query_params)
        return [(row['archive_id'], row['ad_creative_body']) for row in cursor.fetchall()]

    def insert_new_topic_names(self, topic_names):
        """Inserts provide topic names if they do not already exist

        Args:
            topic_names: iterable of str of topic_names.
        """
        cursor = self.get_cursor()
        existing_topic_name_query = ('SELECT topic_name FROM topics')
        cursor.execute(existing_topic_name_query)
        existing_topic_names = {row['topic_name'] for row in cursor.fetchall()}
        new_topic_names = topic_names - existing_topic_names
        topic_name_insert_query = (
            'INSERT INTO topics (topic_name) VALUES %s ON CONFLICT (topic_name) DO NOTHING')
        psycopg2.extras.execute_values(
            cursor,
            topic_name_insert_query,
            # execute_values reqiures a sequence of sequnces, so we make a set of single element
            # tuple out of each name.
            {(topic_name,) for topic_name in new_topic_names})

    def update_advertiser_scores(self, advertiser_score_records):
        """Update/insert advertiser scores to page_metadata table.

        Args:
            advertiser_score_records: iterable of AdvertiserScoreRecord.
        """
        query = (
            'INSERT INTO page_metadata (page_id, advertiser_score) VALUES %s ON CONFLICT (page_id) '
            'DO UPDATE SET advertiser_score = EXCLUDED.advertiser_score')
        insert_template = '(%(page_id)s, %(advertiser_score)s)'
        advertiser_score_record_list = [x._asdict() for x in advertiser_score_records]
        cursor = self.get_cursor()
        psycopg2.extras.execute_values(
            cursor,
            query,
            advertiser_score_record_list,
            template=insert_template)

    def all_topics(self):
        """Get all topics as dict of topics name -> topic ID.

        Return:
            dict of topic name -> topic ID.
        """
        cursor = self.get_cursor()
        cursor.execute('SELECT topic_name, topic_id from topics')
        return {row['topic_name']: row['topic_id'] for row in cursor.fetchall()}

    def insert_ad_topics(self, ad_topic_records):
        """Insert ad topics.

        Args:
            ad_topic_records: list of AdTopicRecord namedtuples.
        """
        cursor = self.get_cursor()
        query = ('INSERT INTO ad_topics (topic_id, archive_id) VALUES %s ON CONFLICT '
                 '(topic_id, archive_id) DO NOTHING')
        insert_template = '(%(topic_id)s, %(archive_id)s)'
        ad_topic_record_list = [x._asdict() for x in ad_topic_records]
        psycopg2.extras.execute_values(
            cursor,
            query,
            ad_topic_record_list,
            template=insert_template,
            page_size=_DEFAULT_PAGE_SIZE)

    def update_ad_types(self, ad_type_map):
        cursor = self.get_cursor()
        insert_funder_query = (
            "INSERT INTO ad_metadata(archive_id, ad_type) VALUES %s ON CONFLICT (archive_id) "
            "DO UPDATE SET ad_type = EXCLUDED.ad_type")
        insert_template = "(%s, %s)"
        psycopg2.extras.execute_values(cursor,
                                       insert_funder_query,
                                       ad_type_map,
                                       template=insert_template,
                                       page_size=_DEFAULT_PAGE_SIZE)

    def update_ad_last_active_date(self, last_active_date, archive_ids):
        cursor = self.get_cursor()
        update_last_active_field_query = (
            'UPDATE impressions SET last_active_date = %(last_active_date)s WHERE archive_id = '
            '%(archive_id)s')
        # execute_batch requires an interable of iterables for arglist
        archive_id_arg_list = [{'last_active_date': last_active_date, 'archive_id': archive_id}
                               for archive_id in archive_ids]
        psycopg2.extras.execute_batch(cursor,
                                      update_last_active_field_query,
                                      archive_id_arg_list,
                                      page_size=_DEFAULT_PAGE_SIZE)
