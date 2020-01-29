import collections
import configparser
import logging
import psycopg2
import psycopg2.extras
import sys

import db_functions

OldAdRecord = collections.namedtuple(
    "AdRecord",
    [
        "archive_id",
        "page_id",
        "page_name",
        "image_url",
        "text",
        "sponsor_label",
        "creation_date",
        "start_date",
        "end_date",
        "is_active",
        "min_impressions",
        "max_impressions",
        "min_spend",
        "max_spend",
        "currency",
        "ad_creative_link_caption",
        "ad_creative_link_description",
        "ad_creative_link_title",
    ],
)
OldPageRecord = collections.namedtuple("PageRecord", ["id", "name"])
OldSnapshotRegionRecord = collections.namedtuple(
    "SnapshotRegionRecord",
    [
        "archive_id",
        "name",
        "min_impressions",
        "max_impressions",
        "min_spend",
        "max_spend",
        "crawl_date",
    ],
)
OldSnapshotDemoRecord = collections.namedtuple(
    "SnapshotDemoRecord",
    [
        "archive_id",
        "age_range",
        "gender",
        "min_impressions",
        "max_impressions",
        "min_spend",
        "max_spend",
        "crawl_date",
    ],
)

NewAdRecord = collections.namedtuple(
    "AdRecord",
    [
        "ad_creation_time",
        "ad_creative_body",
        "ad_creative_link_caption",
        "ad_creative_link_description",
        "ad_creative_link_title",
        "ad_delivery_start_time",
        "ad_delivery_stop_time",
        "ad_snapshot_url",
        "ad_status",
        "archive_id",
        "country_code",
        "currency",
        "first_crawl_time",
        "funding_entity",
        "impressions__lower_bound",
        "impressions__upper_bound",
        "page_id",
        "page_name",
        "publisher_platform",
        "spend__lower_bound",
        "spend__upper_bound",
    ],
)
NewPageRecord = collections.namedtuple("PageRecord", ["id", "name"])
NewSnapshotRegionRecord = collections.namedtuple(
    "SnapshotRegionRecord",
    [
        "archive_id",
        "region",
        "spend_percentage",
        "min_impressions",
        "max_impressions",
        "min_spend",
        "max_spend",
    ],
)
NewSnapshotDemoRecord = collections.namedtuple(
    "SnapshotDemoRecord",
    [
        "archive_id",
        "age_range",
        "gender",
        "spend_percentage",
        "min_impressions",
        "max_impressions",
        "min_spend",
        "max_spend",
    ],
)


def get_db_connection(postgres_config):
    host = postgres_config['HOST']
    dbname = postgres_config['DBNAME']
    user = postgres_config['USER']
    password = postgres_config['PASSWORD']
    port = postgres_config['PORT']
    dbauthorize = "host=%s dbname=%s user=%s password=%s port=%s" % (
        host, dbname, user, password, port)
    return psycopg2.connect(dbauthorize)


class SchemaMigrator:

  def __init__(src_db_connection, dest_db_connection, batch_size):
    self.batch_size = batch_size
    self.src_db_connection = src_db_connection
    self.dest_db_connection = dest_db_connection
    self.dest_db_interface = db_functions.DBInterface(self.dest_db_connection)

  def get_src_cursor(self):
    return self.src_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

  def get_dest_cursor(self):
    return self.dest_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

  def run_migration(self):
    logging.info('Starting migration.')
    # migrate page_id as it is foreign key in a lot of tables
    self.migrate_page_table()
    # migrate archive_id as it is second most refrenced foreign key
    self.migrate_ads_table()
    # migrate impressions since that only relies on archive_id

    # migrate funder_id as it is next most refrenced foreign key
    # computations required for demographic and regional impressions and
    # spending
    # maybe demo and regional impressions -> results

  def migrate_pages_table(self):
    logging.info('Migrationg pages table.')
    src_cursor = self.get_src_cursor()
    src_cursor.array_size = self.batch_size
    src_pages_query = 'SELECT (page_id, page_name) from pages'
    srcs_cursor.execute(src_cursor.mogrify(src_page_query))
    fetched_rows = src_cursor.fetchmany()
    num_rows_processed = 0
    while fetched_row:
      page_records = []
      for row in fetched_row:
        page_records.append(NewPageRecord(page_id=row['page_id'],
          page_name=row['page_name']))

      self.dest_db_interface.dest_db_interface.insert_pages(page_records)
      num_rows_processed += len(page_records)
      logging.info('Migrated %d page rows so far.', num_rows_processed)

    logging.info('Migrated %d page rows total.', num_rows_processed)

  def migrate_ads_table(self):
    logging.info('Migrationg ads table.')
    src_cursor = self.get_src_cursor()
    src_cursor.array_size = self.batch_size
    srcs_ads_query = 'SELECT * FROM ads;'
    srcs_cursor.execute(src_cursor.mogrify(src_ads_query))
    fetched_rows = src_cursor.fetchmany()
    num_rows_processed = 0
    while fetched_rows:
      ad_records = []
      for row in fetched_rows:
        old_ad_record = OldAdRecord(row)
        ad_records.apeend(NewAdRecord(
        ad_creation_time=old_ad_record.creation_date,
        ad_creative_body=old_ad_record.text,
        ad_creative_link_caption=old_ad_record.ad_creative_link_caption,
        ad_creative_link_description=old_ad_record.ad_creative_link_description,
        ad_creative_link_title=old_ad_record.ad_creative_link_title,
        ad_delivery_start_time=old_ad_record.start_date,
        ad_delivery_stop_time=old_ad_record.end_date,
        ad_snapshot_url=row['snapshot_url'],
        ad_status=old_ad_record.is_active,
        archive_id=old_ad_record.archive_id,
        country_code='US',
        currency='USD',
        first_crawl_time=None,
        funding_entity=old_ad_record.roe['funding_entity'],
        page_id=old_ad_record.page_id,
        page_name=old_ad_record.page_name,
        publisher_platform=None))

      self.dest_db_interface.dest_db_interface.insert_new_ads(ad_records)
      num_rows_processed += len(ad_records)
      logging.info('Migrated %d page rows so far.', num_rows_processed)

    logging.info('Migrated %d page rows total.', num_rows_processed)

  def migrate_ads_table(self):
    logging.info('Migrationg ads table.')
    src_cursor = self.get_src_cursor()
    src_cursor.array_size = self.batch_size
    srcs_ads_query = 'SELECT * FROM ads;'
    srcs_cursor.execute(src_cursor.mogrify(src_ads_query))
    fetched_rows = src_cursor.fetchmany()
    num_rows_processed = 0
    while fetched_rows:
      ad_records = []
      for row in fetched_rows:
        old_ad_record = OldAdRecord(row)
        ad_records.apeend(NewAdRecord(
        ad_creation_time=old_ad_record.creation_date,
        ad_creative_body=old_ad_record.text,
        ad_creative_link_caption=old_ad_record.ad_creative_link_caption,
        ad_creative_link_description=old_ad_record.ad_creative_link_description,
        ad_creative_link_title=old_ad_record.ad_creative_link_title,
        ad_delivery_start_time=old_ad_record.start_date,
        ad_delivery_stop_time=old_ad_record.end_date,
        ad_snapshot_url=row['snapshot_url'],
        ad_status=old_ad_record.is_active,
        archive_id=old_ad_record.archive_id,
        country_code='US',
        currency='USD',
        first_crawl_time=None,
        funding_entity=old_ad_record.roe['funding_entity'],
        page_id=old_ad_record.page_id,
        page_name=old_ad_record.page_name,
        publisher_platform=None))

      self.dest_db_interface.dest_db_interface.insert_new_ads(ad_records)
      num_rows_processed += len(ad_records)
      logging.info('Migrated %d page rows so far.', num_rows_processed)

    logging.info('Migrated %d page rows total.', num_rows_processed)

  def migrate_impressions_table(self):
    logging.info('Migrationg pages table.')
    src_cursor = self.get_src_cursor()
    src_cursor.array_size = self.batch_size
    src_pages_query = 'SELECT (page_id, page_name) from pages'
    srcs_cursor.execute(src_cursor.mogrify(src_page_query))
    fetched_rows = src_cursor.fetchmany()
    num_rows_processed = 0
    while fetched_row:
      page_records = []
      for row in fetched_row:
        page_records.append(NewPageRecord(page_id=row['page_id'],
          page_name=row['page_name']))

      self.dest_db_interface.dest_db_interface.insert_pages(page_records)
      num_rows_processed += len(page_records)
      logging.info('Migrated %d page rows so far.', num_rows_processed)

    logging.info('Migrated %d page rows total.', num_rows_processed)

def main(src_db_connection, dest_db_connection, batch_size=1000):
  schema_migrator = SchemaMigraton(src_db_connection, dest_db_connection,
      batch_size)
  schema_migrator.run_migration()


if __name__ == '__main__':
  if len(sys.argv) < 2:
      exit(f"Usage:python3 {sys.argv[0]} generic_fb_collector.cfg")

  config = configparser.ConfigParser()
  config.read(sys.argv[1])
  logging.basicConfig(handlers=[logging.FileHandler("schema_migrator.log"),
                            logging.StreamHandler()],
                      format='[%(levelname)s\t%(asctime)s] %(message)s',
                      level=logging.INFO)
  src_db_config = config['SRC_POSTGRES']
  src_db_connection = get_db_connection(src_db_config)
  logging.info('Established connection to src database: %s',
      src_db_connection.dsn)
  dest_db_config = config['DEST_POSTGRES']
  dest_db_connection = get_db_connection(dest_db_config)
  logging.info('Established connection to src database: %s',
      dest_db_connection.dsn)
  main(src_db_connection, dest_db_connection)
  src_db_connection.close()
  dest_db_connection.close()
