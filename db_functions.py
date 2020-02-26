import psycopg2
import psycopg2.extras


class DBInterface():

    def __init__(self, connection):
        self.connection = connection

    def get_cursor(self):
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

    def existing_pages(self):
        cursor = self.get_cursor()
        existing_pages_query = "select page_id, page_name from pages;"
        cursor.execute(existing_pages_query)
        existing_pages = set()
        for row in cursor:
            existing_pages.add(row['page_id'])
        return existing_pages

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
        existing_ad_clusters_query = 'SELECT ad_creative_id, ad_cluster_id FROM ad_clusters VALUES'
        cursor.execute(existing_ad_clusters_query)
        existing_ad_clusters = dict()
        for row in cursor:
            existing_ad_clusters[row['ad_creative_id']] = row['ad_cluster_id']
        return existing_ad_clusters

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
        """Returns list of ad creative image simhashes.
        """
        cursor = self.get_cursor()
        duplicate_simhash_query = (
            'SELECT archive_id, image_sim_hash FROM ad_creatives WHERE image_sim_hash IS NOT NULL;'
        )
        cursor.execute(duplicate_simhash_query)
        results = cursor.fetchall()
        return dict([(row['archive_id'], row['image_sim_hash']) for row in results])

    def all_ad_creative_text_simhashes(self):
        """Returns list of ad creative text simhashes.
        """
        cursor = self.get_cursor()
        duplicate_simhash_query = (
            'SELECT archive_id, text_sim_hash FROM ad_creatives WHERE text_sim_hash IS NOT NULL;'
        )
        cursor.execute(duplicate_simhash_query)
        results = cursor.fetchall()
        return dict([(row['archive_id'], row['text_sim_hash']) for row in results])

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
            'SELECT DISTINCT(archive_id) FROM ad_creatives WHERE text_sim_hash = %s')
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

    def all_archive_ids_without_image_hash(self):
        """Get ALL ad archive IDs that do not exist in ad_images table.

        Args:
        cursor: pyscopg2.Cursor DB cursor for query execution.
        Returns:
        list of archive IDs (str).
        """
        cursor = self.get_cursor()
        archive_ids_sample_query = cursor.mogrify(
            'SELECT archive_id from ads WHERE archive_id NOT IN (select archive_id FROM ad_images) '
            'ORDER BY ad_creation_time DESC')
        cursor.execute(archive_ids_sample_query)
        results = cursor.fetchall()
        return [row['archive_id'] for row in results]

    def n_archive_ids_without_image_hash(self, max_archive_ids=200):
        """Get ad archive IDs that do not exist in ad_images table.

        Args:
        cursor: pyscopg2.Cursor DB cursor for query execution.
        max_archive_ids: int, limit on how many IDs to query DB for.
        Returns:
        list of archive IDs (str).
        """
        cursor = self.get_cursor()
        archive_ids_sample_query = cursor.mogrify(
            'SELECT archive_id from ads WHERE archive_id NOT IN (select archive_id FROM ad_images) '
            'ORDER BY ad_creation_time DESC LIMIT %s')
        cursor.execute(archive_ids_sample_query, (max_archive_ids,))
        return [row['archive_id'] for row in cursor.fetchall()]

    def insert_funding_entities(self, new_funders):
        cursor = self.get_cursor()
        insert_funder_query = "INSERT INTO funder_metadata(funder_name) VALUES %s;"
        insert_template = "(%s)"
        psycopg2.extras.execute_values(cursor,
                                       insert_funder_query,
                                       new_funders,
                                       template=insert_template,
                                       page_size=250)

    def insert_pages(self, new_pages):
        cursor = self.get_cursor()
        insert_page_query = ("INSERT INTO pages(page_id, page_name) VALUES %s "
                             "on conflict (page_id) do nothing;")
        insert_template = "(%(id)s, %(name)s)"
        new_page_list = [x._asdict() for x in new_pages]

        psycopg2.extras.execute_values(cursor,
                                       insert_page_query,
                                       new_page_list,
                                       template=insert_template,
                                       page_size=250)

    def insert_page_metadata(self, new_page_metadata):
        cursor = self.get_cursor()
        insert_page_metadata_query = (
            "INSERT INTO page_metadata(page_id, page_url, federal_candidate) VALUES %s "
            "on conflict (page_id) do nothing;")
        insert_template = "(%(id)s, %(url)s, %(federal_candidate)s)"
        new_page_metadata_list = [x._asdict() for x in new_page_metadata]
        psycopg2.extras.execute_values(
            cursor, insert_page_metadata_query, new_page_metadata_list, template=insert_template, page_size=250)


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
                                       page_size=250)

        ad_insert_query = (
            "INSERT INTO ad_countries(archive_id, country_code) VALUES %s on conflict (archive_id, "
            "country_code) do nothing;")
        insert_template = ("(%(archive_id)s, %(country_code)s)")
        psycopg2.extras.execute_values(cursor,
                                       ad_insert_query,
                                       new_ad_list,
                                       template=insert_template,
                                       page_size=250)

        # Mark newly found archive_id as needing scrape.
        snapshot_metadata_insert_query = (
            "INSERT INTO ad_snapshot_metadata (archive_id, needs_scrape) "
            "VALUES %s on conflict (archive_id) do nothing;")
        insert_template = ("(%(archive_id)s, TRUE)")
        psycopg2.extras.execute_values(cursor,
                                       snapshot_metadata_insert_query,
                                       new_ad_list,
                                       template=insert_template,
                                       page_size=250)

    def insert_new_impressions(self, new_impressions):
        cursor = self.get_cursor()
        impressions_insert_query = (
            "INSERT INTO impressions(archive_id, ad_status, min_spend, max_spend, min_impressions, "
            "max_impressions) VALUES %s on conflict (archive_id) do update set "
            "ad_status = EXCLUDED.ad_status, min_spend = EXCLUDED.min_spend, "
            "max_spend = EXCLUDED.max_spend, min_impressions = EXCLUDED.min_impressions, "
            "max_impressions = EXCLUDED.max_impressions;")

        insert_template = (
            "(%(archive_id)s, %(ad_status)s , %(spend__lower_bound)s, %(spend__upper_bound)s, "
            "%(impressions__lower_bound)s , %(impressions__upper_bound)s)")
        new_impressions_list = ([
            impression._asdict() for impression in new_impressions
        ])

        psycopg2.extras.execute_values(cursor,
                                       impressions_insert_query,
                                       new_impressions_list,
                                       template=insert_template,
                                       page_size=250)

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
                                       page_size=250)

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
                                       page_size=250)

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
                                       page_size=250)
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
                                       page_size=250)

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
                                      page_size=250)


    def insert_ad_creative_records(self, ad_creative_records):
        cursor = self.get_cursor()
        # First insert ad creatives to ad_creatives table.
        insert_query = (
            'INSERT INTO ad_creatives(archive_id, ad_creative_body, ad_creative_link_url, '
            'ad_creative_link_title, ad_creative_link_caption, ad_creative_link_description, '
            'text_sha256_hash, text_sim_hash, image_downloaded_url, image_bucket_path, '
            'image_sim_hash, image_sha256_hash) VALUES %s ON CONFLICT (archive_id, '
            'text_sha256_hash, image_sha256_hash) DO NOTHING')
        insert_template = (
            '(%(archive_id)s, %(ad_creative_body)s, %(ad_creative_link_url)s, '
            '%(ad_creative_link_title)s, %(ad_creative_link_caption)s, '
            '%(ad_creative_link_description)s, %(text_sha256_hash)s, %(text_sim_hash)s, '
            '%(image_downloaded_url)s, %(image_bucket_path)s, %(image_sim_hash)s, '
            '%(image_sha256_hash)s)')
        ad_creative_record_list = [x._asdict() for x in ad_creative_records]
        psycopg2.extras.execute_values(cursor,
                                       insert_query,
                                       ad_creative_record_list,
                                       template=insert_template,
                                       page_size=250)

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
                                       page_size=250)

    def cluster_ids(self, country, start_time, end_time):
        """ Return cluster_ids for all clusters which were active/started in a certain timeframe. """
        # TODO: Implement this to fetch clusters for the given country in the last 
        return []
