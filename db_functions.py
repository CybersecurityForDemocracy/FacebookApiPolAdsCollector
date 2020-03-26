"""Encapsulation of database read, write, and update logic."""
from collections import namedtuple
import logging

import psycopg2
import psycopg2.extras

EntityRecord = namedtuple('EntityRecord', ['name', 'type'])

_DEFAULT_PAGE_SIZE = 250

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
        existing_ad_clusters_query = 'SELECT archive_id, ad_cluster_id FROM ad_clusters VALUES'
        cursor.execute(existing_ad_clusters_query)
        existing_ad_clusters = dict()
        for row in cursor:
            existing_ad_clusters[row['archive_id']] = row['ad_cluster_id']
        return existing_ad_clusters

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
        """Returns list of ad creative image simhashes.
        """
        cursor = self.get_cursor()
        duplicate_simhash_query = (
            'SELECT archive_id, image_sim_hash FROM ad_creatives WHERE image_sim_hash IS NOT NULL;'
        )
        cursor.execute(duplicate_simhash_query)
        results = cursor.fetchall()
        return {row['archive_id']: row['image_sim_hash'] for row in results}

    def all_ad_creative_text_simhashes(self):
        """Returns list of ad creative text simhashes.
        """
        cursor = self.get_cursor()
        duplicate_simhash_query = (
            'SELECT archive_id, text_sim_hash FROM ad_creatives WHERE text_sim_hash IS NOT NULL;'
        )
        cursor.execute(duplicate_simhash_query)
        results = cursor.fetchall()
        return {row['archive_id']: row['text_sim_hash'] for row in results}

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
                                       page_size=_DEFAULT_PAGE_SIZE)

    def insert_page_metadata(self, new_page_metadata):
        cursor = self.get_cursor()
        insert_page_metadata_query = (
            "INSERT INTO page_metadata(page_id, page_url, federal_candidate) VALUES %s "
            "on conflict (page_id) do nothing;")
        insert_template = "(%(id)s, %(url)s, %(federal_candidate)s)"
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
                                       page_size=_DEFAULT_PAGE_SIZE)

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
