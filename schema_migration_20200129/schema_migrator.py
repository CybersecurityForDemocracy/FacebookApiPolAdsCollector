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

FunderRecord = collections.namedtuple(
    "FunderRecord",
    [
      "funder_id",
      "funder_name",
      "funder_type",
      "parent_id",
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
    cursor = self.src_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.array_size = self.batch_size
    return cursor

  def get_dest_cursor(self):
    return self.dest_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

  def run_migration(self):
    logging.info('Starting migration.')
    # migrate page_id as it is foreign key in a lot of tables
    self.migrate_page_table()

    # migrate archive_id as it is second most refrenced foreign key. Also,
    # migrate impressions for archive IDs.
    self.migrate_ads_and_impressions_table()

    # migrate funder_id as it is next most refrenced foreign key
    self.migrate_funder_table()

    # computations required for demographic and regional impressions and
    # spending
    # maybe demo and regional impressions -> results

  def migrate_pages_table(self):
    logging.info('Migrating pages table.')
    src_cursor = self.get_src_cursor()
    src_pages_query = 'SELECT (page_id, page_name) from pages'
    srcs_cursor.execute(src_cursor.mogrify(src_pages_query))
    fetched_rows = src_cursor.fetchmany()
    num_rows_processed = 0
    while fetched_rows:
      page_records = []
      for row in fetched_rows:
        page_records.append(NewPageRecord(page_id=row['page_id'],
          page_name=row['page_name']))

      self.dest_db_interface.dest_db_interface.insert_pages(page_records)
      num_rows_processed += len(page_records)
      logging.info('Migrated %d page rows so far.', num_rows_processed)

    logging.info('Migrated %d page rows total.', num_rows_processed)

  def migrate_ads_and_impressions_table(self):
    logging.info('Migrating ads table.')
    src_cursor = self.get_src_cursor()
    srcs_ads_query = 'SELECT * FROM ads;'
    srcs_cursor.execute(src_cursor.mogrify(src_ads_query))
    num_rows_processed = 0
    fetched_rows = src_cursor.fetchmany()
    while fetched_rows:
      ad_records = []
      ad_status = {}
      for row in fetched_rows:
        ad_status[row['arcive_id']] = row['ad_status']
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
      self.migrate_impressions_for_archive_id_batch(ad_status)
      num_rows_processed += len(ad_records)
      logging.info('Migrated %d page rows so far.', num_rows_processed)

    logging.info('Migrated %d page rows total.', num_rows_processed)

  def migrate_impressions_for_archive_id_batch(self, archive_id_to_ad_status):
    logging.info('Migrationg impressions for archive ID batch.')
    src_cursor = self.get_src_cursor()
    archive_ids = [str(k) for k in archive_id_to_ad_status]
    src_impressions_query = ('SELECT * FROM impressions WHERE archive_id IN '
      '(%s)' % (','.join(archive_ids)))
    srcs_cursor.execute(src_cursor.mogrify(src_impressions_query))

    num_rows_processed = 0
    impression_records = []
    archive_ids_in_results = set()
    fetched_rows = src_cursor.fetchall()
    for row in fetched_row:
      archive_id = row['archive_id']
      archive_ids_in_results.add(archive_id)
      impression_records.append(NewAdRecord(archive_id=archive_id,
        ad_status=archive_id_to_ad_status[archive_id],
        impressions__lower_bound=row['min_impressions'],
        impressions__upper_bound=row['max_impressions'],
        spend__lower_bound=row['min_spend'],
        spend__upper_bound=row['max_spend']))

    archive_ids_to_fetch = set(archive_id_to_ad_status.keys())
    if archive_ids_in_results != archive_ids_to_fetch:
      logging.error('Did not get impressions for archive IDs: %s. IMPRESSIONS '
          'TABLE MIGRATION MAY BE INCOMPLETE.',
          archive_ids_to_fetch.difference(archive_ids_in_results))

    self.dest_db_interface.dest_db_interface.insert_impressions(impression_records)
    num_rows_processed += len(impression_records)
    logging.info('Migrated %d impression rows for %d archive IDs.',
        num_rows_processed, len(archive_ids))

  def migrate_funder_table(self):
    logging.info('Migrationg funder table.')
    src_cursor = self.get_src_cursor()
    src_sponsor_query = src_cursor.mogrify('SELECT * FROM ad_sponsors')
    srcs_curosr.execute(src_sponsor_query)
    fetched_rows = src_cursor.fetchmany()
    num_rows_processed = 0
    while fetched_rows:
      funder_records = []
      for row in fetched_rows:
        funder_records.append(
            FunderRecord(
                funder_id=row['id'],
                funder_name=row['name'],
                funder_type=row['nyu_category'],
                parent_id=row['parent_ad_sponsor_id']))

      insert_funder_query = ("INSERT INTO funder_metadata(funder_id, "
          "funder_name, funder_type, parent_id) VALUES %s;")
      insert_template = ("(%(funder_id)s, %(funder_name)s, %(funder_type)s, "
          "%(parent_id)s)")
      psycopg2.extras.execute_values(
          cursor, insert_funder_query, funder_records, template=insert_template,
          page_size=250)
      num_rows_processed += len(funder_records)

    logging.info('Migrated %d funder rows total.', num_rows_processed)




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
