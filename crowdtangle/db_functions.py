"""Encapsulation of database read, write, and update logic."""
import logging

import psycopg2
import psycopg2.extras
from psycopg2 import sql

_DEFAULT_CROWDTANGLE_PAGE_SIZE = 1000


class CrowdTangleDBInterface:
    def __init__(self, connection):
        self.connection = connection

    def get_cursor(self, real_dict_cursor=False):
        if real_dict_cursor:
            return self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        return self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def upsert_posts(self, posts):
        cursor = self.get_cursor()
        insert_template = (
            '(%(id)s, %(account_id)s, %(branded_content_sponsor_account_id)s, %(message)s, '
            '%(title)s,%(platform)s, %(platform_id)s, %(post_url)s, %(subscriber_count)s, '
            '%(type)s, %(updated)s, %(video_length_ms)s, %(image_text)s, %(legacy_id)s, '
            '%(caption)s, %(link)s, %(date)s, %(description)s, %(score)s, %(live_video_status)s, '
            'CURRENT_TIMESTAMP)')
        insert_posts_query = (
            '''INSERT INTO posts(id, account_id, branded_content_sponsor_account_id, message, title,
            platform, platform_id, post_url, subscriber_count, type, updated, video_length_ms,
            image_text, legacy_id, caption, link, date, description, score, live_video_status,
            last_modified_time) VALUES %s ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id,
            account_id = EXCLUDED.account_id,
            branded_content_sponsor_account_id = EXCLUDED.branded_content_sponsor_account_id,
            message = EXCLUDED.message, title = EXCLUDED.title, platform = EXCLUDED.platform,
            platform_id = EXCLUDED.platform_id, post_url = EXCLUDED.post_url,
            subscriber_count = EXCLUDED.subscriber_count, type = EXCLUDED.type,
            updated = EXCLUDED.updated, video_length_ms = EXCLUDED.video_length_ms,
            image_text = EXCLUDED.image_text, legacy_id = EXCLUDED.legacy_id,
            caption = EXCLUDED.caption, link = EXCLUDED.link, date = EXCLUDED.date,
            description = EXCLUDED.description, score = EXCLUDED.score,
            live_video_status = EXCLUDED.live_video_status, last_modified_time = CURRENT_TIMESTAMP
            WHERE posts.id = EXCLUDED.id AND posts.updated < EXCLUDED.updated;''')
        psycopg2.extras.execute_values(cursor,
                                       insert_posts_query,
                                       [x._asdict() for x in posts],
                                       template=insert_template,
                                       page_size=_DEFAULT_CROWDTANGLE_PAGE_SIZE)
        logging.debug('upsert_posts statusmessage: %s', cursor.statusmessage)
        logging.debug('upsert_posts query: %s', cursor.query)

    def upsert_accounts(self, accounts):
        cursor = self.get_cursor()
        insert_template = (
            '(%(id)s, %(account_type)s, %(handle)s, %(name)s, %(page_admin_top_country)s, '
            '%(platform)s, %(platform_id)s, %(profile_image)s, %(subscriber_count)s, %(url)s, '
            '%(verified)s, %(updated)s, CURRENT_TIMESTAMP)')
        insert_accounts_query = (
            '''INSERT INTO accounts(id, account_type, handle, name, page_admin_top_country,
            platform, platform_id, profile_image, subscriber_count, url, verified, updated,
            last_modified_time) VALUES %s ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id,
            account_type = EXCLUDED.account_type, handle = EXCLUDED.handle, name = EXCLUDED.name,
            page_admin_top_country = EXCLUDED.page_admin_top_country, platform = EXCLUDED.platform,
            platform_id = EXCLUDED.platform_id, profile_image = EXCLUDED.profile_image,
            subscriber_count = EXCLUDED.subscriber_count, url = EXCLUDED.url,
            verified = EXCLUDED.verified, updated = EXCLUDED.updated,
            last_modified_time = EXCLUDED.last_modified_time
            WHERE accounts.id = EXCLUDED.id AND accounts.updated < EXCLUDED.updated;''')
        try:
            psycopg2.extras.execute_values(cursor,
                                           insert_accounts_query,
                                           [x._asdict() for x in accounts],
                                           template=insert_template,
                                           page_size=_DEFAULT_CROWDTANGLE_PAGE_SIZE)
        except psycopg2.Error as err:
            logging.warning('Error %s in query:\n%s', err, cursor.query)
            raise
        logging.debug('upsert_accounts statusmessage: %s', cursor.statusmessage)
        logging.debug('upsert_accounts query: %s', cursor.query)

    def upsert_statistics(self, statistics_actual, statistics_expected):
        cursor = self.get_cursor()
        insert_template = (
            '(%(post_id)s, %(updated)s, %(angry_count)s, %(comment_count)s, %(favorite_count)s, '
            '%(haha_count)s, %(like_count)s, %(love_count)s, %(sad_count)s, %(share_count)s, '
            '%(up_count)s, %(wow_count)s, %(thankful_count)s, %(care_count)s,'
            'CURRENT_TIMESTAMP)')
        insert_statisitics_query_template = sql.SQL(
            '''INSERT INTO {table_name} (post_id, updated, angry_count, comment_count,
            favorite_count, haha_count, like_count, love_count, sad_count, share_count, up_count,
            wow_count, thankful_count, care_count, last_modified_time) VALUES %s ON CONFLICT
            (post_id) DO UPDATE SET post_id = EXCLUDED.post_id, updated = EXCLUDED.updated,
            angry_count = EXCLUDED.angry_count, comment_count = EXCLUDED.comment_count,
            favorite_count = EXCLUDED.favorite_count, haha_count = EXCLUDED.haha_count,
            like_count = EXCLUDED.like_count, love_count = EXCLUDED.love_count,
            sad_count = EXCLUDED.sad_count, share_count = EXCLUDED.share_count,
            up_count = EXCLUDED.up_count, wow_count = EXCLUDED.wow_count,
            thankful_count = EXCLUDED.thankful_count, care_count = EXCLUDED.care_count,
            last_modified_time = EXCLUDED.last_modified_time
            WHERE {table_name}.post_id = EXCLUDED.post_id AND
            {table_name}.updated < EXCLUDED.updated''')
        insert_statisitics_actual_query = insert_statisitics_query_template.format(
            table_name=sql.Identifier('post_statistics_actual'))
        insert_statisitics_expected_query = insert_statisitics_query_template.format(
            table_name=sql.Identifier('post_statistics_expected'))
        psycopg2.extras.execute_values(cursor,
                                       insert_statisitics_actual_query,
                                       [x._asdict() for x in statistics_actual],
                                       template=insert_template,
                                       page_size=_DEFAULT_CROWDTANGLE_PAGE_SIZE)
        logging.debug('upsert_statistics post_statistics_actual table statusmessage: %s',
                     cursor.statusmessage)
        logging.debug('upsert_statistics post_statistics_actual table query: %s', cursor.query)
        psycopg2.extras.execute_values(cursor,
                                       insert_statisitics_expected_query,
                                       [x._asdict() for x in statistics_expected],
                                       template=insert_template,
                                       page_size=_DEFAULT_CROWDTANGLE_PAGE_SIZE)
        logging.debug('upsert_statistics post_statistics_expected table statusmessage: %s',
                     cursor.statusmessage)
        logging.debug('upsert_statistics post_statistics_expected table query: %s', cursor.query)

    def upsert_expanded_links(self, expanded_links):
        cursor = self.get_cursor()
        insert_template = ('(%(post_id)s, %(expanded)s, %(original)s, CURRENT_TIMESTAMP)')
        insert_query = (
            '''INSERT INTO expanded_links (post_id, expanded, original, last_modified_time)
            VALUES %s ON CONFLICT DO NOTHING''')
        psycopg2.extras.execute_values(cursor,
                                       insert_query,
                                       [x._asdict() for x in expanded_links],
                                       template=insert_template,
                                       page_size=_DEFAULT_CROWDTANGLE_PAGE_SIZE)
        logging.debug('upsert_expaneded_links statusmessage: %s', cursor.statusmessage)
        logging.debug('upsert_expaneded_links query: %s', cursor.query)

    def upsert_media(self, media_records):
        cursor = self.get_cursor()
        insert_template = (
            '(%(post_id)s, %(url_full)s, %(url)s, %(width)s, %(height)s, '
            '%(type)s, CURRENT_TIMESTAMP)')
        insert_query = (
            '''INSERT INTO media (post_id, url_full, url, width, height, type,
             last_modified_time) VALUES %s ON CONFLICT DO NOTHING''')
        psycopg2.extras.execute_values(cursor,
                                       insert_query,
                                       [x._asdict() for x in media_records],
                                       template=insert_template,
                                       page_size=_DEFAULT_CROWDTANGLE_PAGE_SIZE)
        logging.debug('upsert_media statusmessage: %s', cursor.statusmessage)
        logging.debug('upsert_media query: %s', cursor.query)
