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
            ads_to_end_time_map[row['archive_id']]=row['ad_delivery_stop_time']
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

    def all_archive_ids_without_creatives_data(self):
      """Get ALL ad archive IDs that do not exist in ad_creatives table.

      Args:
        cursor: pyscopg2.Cursor DB cursor for query execution.
      Returns:
        list of archive IDs (str).
      """
      cursor = self.get_cursor()
      archive_ids_sample_query = cursor.mogrify('SELECT archive_id from ads '
          'WHERE archive_id NOT IN (select archive_id FROM ad_creatives) ORDER '
          'BY ad_creation_time DESC')
      cursor.execute(archive_ids_sample_query)
      results = cursor.fetchall()
      return [row['archive_id'] for row in results]

    def n_archive_ids_without_creatives_data(self, max_archive_ids=200):
      """Get ad archive IDs that do not exist in ad_creatives table.

      Args:
        cursor: pyscopg2.Cursor DB cursor for query execution.
        max_archive_ids: int, limit on how many IDs to query DB for.
      Returns:
        list of archive IDs (str).
      """
      cursor = self.get_cursor()
      archive_ids_sample_query = cursor.mogrify('SELECT archive_id from ads '
          'WHERE archive_id NOT IN (select archive_id FROM ad_creatives) '
          'ORDER BY ad_creation_time DESC LIMIT %s;' % max_archive_ids)
      cursor.execute(archive_ids_sample_query)
      results = cursor.fetchall()
      return [row['archive_id'] for row in results]

    def insert_funding_entities(self, new_funders):
        cursor = self.get_cursor()
        insert_funder_query = "INSERT INTO funder_metadata(funder_name) VALUES %s;"
        insert_template = "(%s)"
        psycopg2.extras.execute_values(
            cursor, insert_funder_query, new_funders, template=insert_template, page_size=250)

    def insert_pages(self, new_pages):
        cursor = self.get_cursor()
        insert_page_query = (
            "INSERT INTO pages(page_id, page_name) VALUES %s "
            "on conflict (page_id) do nothing;")
        insert_template = "(%(id)s, %(name)s)"
        new_page_list = [x._asdict() for x in new_pages]
        psycopg2.extras.execute_values(
            cursor, insert_page_query, new_page_list, template=insert_template, page_size=250)


    def insert_new_ads(self, new_ads):
        cursor = self.get_cursor()
        ad_insert_query = (
            "INSERT INTO ads(archive_id, ad_creative_body, ad_creation_time, ad_delivery_start_time, "
            "ad_delivery_stop_time, page_id, currency, ad_creative_link_caption, "
            "ad_creative_link_title, ad_creative_link_description, ad_snapshot_url, funding_entity) "
            "VALUES %s on conflict (archive_id) do nothing;")
        insert_template = (
            "(%(archive_id)s, %(ad_creative_body)s, %(ad_creation_time)s, %(ad_delivery_start_time)s, "
            "%(ad_delivery_stop_time)s, %(page_id)s, %(currency)s, %(ad_creative_link_caption)s, "
            "%(ad_creative_link_title)s, %(ad_creative_link_description)s, %(ad_snapshot_url)s, %(funding_entity)s)")
        new_ad_list = [x._asdict() for x in new_ads]
        psycopg2.extras.execute_values(
            cursor, ad_insert_query, new_ad_list, template=insert_template, page_size=250)
        ad_insert_query = (
            "INSERT INTO ad_countries(archive_id, country_code) "
            "VALUES %s on conflict (archive_id, country_code) do nothing;")
        insert_template = (
            "(%(archive_id)s, %(country_code)s)")
        new_ad_list = [x._asdict() for x in new_ads]
        psycopg2.extras.execute_values(
            cursor, ad_insert_query, new_ad_list, template=insert_template, page_size=250)

    def insert_new_impressions(self, new_impressions):
        cursor = self.get_cursor()
        impressions_insert_query = (
            "INSERT INTO impressions(archive_id, ad_status, min_spend, max_spend, "
            "min_impressions, max_impressions) "
            "VALUES %s on conflict (archive_id) do update set "
            "ad_status = EXCLUDED.ad_status, min_spend = EXCLUDED.min_spend, max_spend = EXCLUDED.max_spend, "
            "min_impressions = EXCLUDED.min_impressions, max_impressions = EXCLUDED.max_impressions;")

        insert_template = (
            "(%(archive_id)s, %(ad_status)s , %(spend__lower_bound)s, %(spend__upper_bound)s , "
            "%(impressions__lower_bound)s , %(impressions__upper_bound)s)")
        new_impressions_list = [impression._asdict() for impression in new_impressions]

        psycopg2.extras.execute_values(
            cursor, impressions_insert_query, new_impressions_list, template=insert_template, page_size=250)

    def insert_new_impression_demos(self, new_ad_demo_impressions):
        demo_impressions_list = [impression._asdict() for impression in new_ad_demo_impressions]
        cursor = self.get_cursor()
        impression_demo_insert_query = (
            "INSERT INTO demo_impressions(archive_id, age_group, gender, spend_percentage) "
            "VALUES %s on conflict on constraint unique_demos_per_ad do update set "
            "spend_percentage = EXCLUDED.spend_percentage;")
        insert_template = '(%(archive_id)s, %(age_range)s, %(gender)s, %(spend_percentage)s)'

        psycopg2.extras.execute_values(
            cursor, impression_demo_insert_query, demo_impressions_list, template=insert_template, page_size=250)

        impression_demo_result_insert_query = ("INSERT INTO demo_impression_results("
            "archive_id, age_group, gender, min_impressions, min_spend, "
            "max_impressions, max_spend) "
            "VALUES %s "
            "on conflict on constraint unique_demo_results do update set "
            "min_impressions = EXCLUDED.min_impressions, min_spend = EXCLUDED.min_spend, "
            "max_impressions = EXCLUDED.max_impressions, max_spend = EXCLUDED.max_spend;")
        insert_template = (
            '(%(archive_id)s, %(age_range)s, %(gender)s, %(min_impressions)s , %(min_spend)s,'
            ' %(max_impressions)s , %(max_spend)s)')
        psycopg2.extras.execute_values(
            cursor, impression_demo_result_insert_query, demo_impressions_list, template=insert_template, page_size=250)


    def insert_new_impression_regions(self, new_ad_region_impressions):
        region_impressions_list = [impression._asdict() for impression in new_ad_region_impressions]
        cursor = self.get_cursor()
        impression_region_insert_query = ("INSERT INTO region_impressions("
            "archive_id, region, spend_percentage) VALUES %s "
            "on conflict on constraint unique_regions_per_ad do update set "
            "spend_percentage = EXCLUDED.spend_percentage;")
        insert_template = (
            "(%(archive_id)s, %(region)s, %(spend_percentage)s)")
        psycopg2.extras.execute_values(
            cursor, impression_region_insert_query, region_impressions_list, template=insert_template, page_size=250)
        impression_region_insert_query = ("INSERT INTO region_impression_results("
            "archive_id, region, min_impressions, min_spend, "
            "max_impressions, max_spend) VALUES %s "
            "on conflict on constraint unique_region_results do update set "
            "min_impressions = EXCLUDED.min_impressions, min_spend = EXCLUDED.min_spend, "
            "max_impressions = EXCLUDED.max_impressions, max_spend = EXCLUDED.max_spend;")
        insert_template = (
            "(%(archive_id)s, %(region)s, %(min_impressions)s, %(min_spend)s,"
            " %(max_impressions)s, %(max_spend)s)")
        psycopg2.extras.execute_values(
            cursor, impression_region_insert_query, region_impressions_list, template=insert_template, page_size=250)

    def insert_ad_image_records(self, ad_image_records):
      cursor = self.get_cursor()
      insert_query = ('INSERT INTO ad_images(archive_id, '
          'fetch_time, downloaded_url, bucket_url, sim_hash, sha256_hash) '
          'VALUES %s ON CONFLICT (archive_id, sha256_hash) DO NOTHING')
      insert_template = ('(%(archive_id)s, %(fetch_time)s, '
        '%(downloaded_url)s, %(bucket_url)s,  %(sim_hash)s, %(image_sha256)s)')
      ad_image_record_list = [x._asdict() for x in ad_image_records]
      psycopg2.extras.execute_values(cursor, insert_query, ad_image_record_list,
          template=insert_template, page_size=250)

    def insert_ad_creative_records(self, ad_creative_records):
      cursor = self.get_cursor()
      insert_query = ('INSERT INTO ad_creatives(archive_id, '
          'snapshot_fetch_time, ad_creative_body, text_sha256_hash, '
          'text_sim_hash, image_downloaded_url, image_bucket_path, '
          'image_sim_hash, image_sha256_hash) VALUES %s ')
          # TODO(macpd): figure out how to handle conflicts/updates
          # ON CONFLICT (archive_id, image_sha256_hash) DO NOTHING')
      insert_template = ('(%(archive_id)s, %(snapshot_fetch_time)s, '
        '%(ad_creative_body)s, %(text_sha256_hash)s, %(text_sim_hash)s, '
        '%(image_downloaded_url)s, %(image_bucket_path)s,  %(image_sim_hash)s, '
        '%(image_sha256_hash)s)')
      ad_creative_record_list = [x._asdict() for x in ad_creative_records]
      psycopg2.extras.execute_values(cursor, insert_query, ad_creative_record_list,
          template=insert_template, page_size=250)
