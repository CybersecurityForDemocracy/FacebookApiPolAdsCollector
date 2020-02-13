import collections
import configparser
import logging
import sys

import psycopg2
import psycopg2.extras

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
OldPageRecord = collections.namedtuple("PageRecord", ["id", "name", "url"])
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
NewPageRecord = collections.namedtuple("NewPageRecord", ["id", "name"])
PageMetadataRecord = collections.namedtuple("PageMetadataRecord", ["id", "url", "federal_candidate"])
PageMetadataCategoriesRecord = collections.namedtuple("PageMetadataCategories", ["id", "category"])
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

    def __init__(self, src_db_connection, dest_db_connection, batch_size):
        self.batch_size = batch_size
        self.src_db_connection = src_db_connection
        self.dest_db_connection = dest_db_connection
        self.dest_db_interface = db_functions.DBInterface(
            self.dest_db_connection)

    def get_src_cursor(self):
        cursor = self.src_db_connection.cursor(
            cursor_factory=psycopg2.extras.DictCursor)
        cursor.arraysize = self.batch_size
        return cursor

    def get_dest_cursor(self):
        return self.dest_db_connection.cursor()

    def run_migration(self):
        logging.info('Starting migration.')

        # migrate page_id as it is foreign key in a lot of tables
        self.migrate_pages_table()
        self.dest_db_connection.commit()

        # migrate funder_id as it is next most refrenced foreign key
        funder_id_to_name = self.migrate_funder_table()
        self.dest_db_connection.commit()

        # migrate archive_id as it is second most refrenced foreign key. Also,
        # migrate impressions for archive IDs.
        self.migrate_ads_and_impressions_table(funder_id_to_name)
        self.dest_db_connection.commit()

        # computations required for demographic and regional impressions and
        # spending
        # maybe demo and regional impressions -> results
        self.migrate_demo_impressions_table()
        self.dest_db_connection.commit()
        self.migrate_region_impressions_table()
        self.dest_db_connection.commit()


    def migrate_pages_table(self):
        logging.info('Migrating pages table.')
        src_pages_query = 'SELECT page_id, page_name, url, federal_candidate FROM pages'
        src_cursor = self.get_src_cursor()
        src_cursor.execute(src_cursor.mogrify(src_pages_query))
        logging.info("%d rows retrieved.", src_cursor.rowcount)
        fetched_rows = src_cursor.fetchmany()
        NewPageMetadataCategoriesRecord = collections.namedtuple("PageMetadataCategories", ["id", "category"])

        num_rows_processed = 0
        while fetched_rows:
            page_records = []
            page_metadata_records = []
            for row in fetched_rows:
                logging.debug('Processing row: %r', row)
                page_records.append(
                    NewPageRecord(id=row['page_id'],
                                  name=row['page_name']))
                page_metadata_records.append(PageMetadataRecord(id=row['page_id'],
                                                                url=row['url'],
                                                                federal_candidate=row['federal_candidate']))

            self.dest_db_interface.insert_pages(page_records)
            self.dest_db_interface.insert_page_metadata(page_metadata_records)
            num_rows_processed += len(page_records)
            logging.info('Migrated %d page rows so far.', num_rows_processed)
            fetched_rows = src_cursor.fetchmany()


        logging.info('Migrated %d page rows total.', num_rows_processed)

    def migrate_funder_table(self):
        logging.info('Migrationg funder table.')
        src_cursor = self.get_src_cursor()
        src_sponsor_query = src_cursor.mogrify('SELECT * FROM ad_sponsors')
        src_cursor.execute(src_sponsor_query)
        fetched_rows = src_cursor.fetchmany()
        num_rows_processed = 0
        id_to_name = {}
        while fetched_rows:
            funder_records = []
            for row in fetched_rows:
                id_to_name[row['id']] = row['name']
                funder_records.append(
                    FunderRecord(funder_id=row['id'],
                                 funder_name=row['name'],
                                 funder_type=row['nyu_category'],
                                 parent_id=row['parent_ad_sponsor_id']))


            insert_funder_query = (
                "INSERT INTO funder_metadata( "
                "funder_name, funder_type, parent_id) VALUES %s "
                "on conflict(funder_name) do nothing;")
            insert_template = (
                "(%(funder_name)s, %(funder_type)s, "
                "%(parent_id)s)")
            funder_records_list = [x._asdict() for x in funder_records]
            psycopg2.extras.execute_values(self.get_dest_cursor(),
                                           insert_funder_query,
                                           funder_records_list,
                                           template=insert_template,
                                           page_size=250)
            num_rows_processed += len(funder_records)
            fetched_rows = src_cursor.fetchmany()

        logging.info('Migrated %d funder rows total.', num_rows_processed)
        return id_to_name

    def migrate_ads_and_impressions_table(self, funder_id_to_name):
        logging.info('Migrating ads table.')
        src_cursor = self.get_src_cursor()
        src_ads_query = ('SELECT creation_date, text, link_caption, link_description, link_title, start_date, end_date, snapshot_url, is_active, archive_id, '
                        'country_code, currency, page_id, ad_sponsors.name FROM ads LEFT JOIN ad_sponsors on ads.ad_sponsor_id = ad_sponsors.id;')
        src_cursor.execute(src_cursor.mogrify(src_ads_query))
        num_rows_processed = 0
        fetched_rows = src_cursor.fetchmany()
        while fetched_rows:
            archive_id_to_ad_record = {}
            for row in fetched_rows:
                logging.debug('Processing row: %r', row)
                new_record = NewAdRecord(
                    ad_creation_time=row['creation_date'],
                    ad_creative_body=row['text'],
                    ad_creative_link_caption=row['link_caption'],
                    ad_creative_link_description=row['link_description'],
                    ad_creative_link_title=row['link_title'],
                    ad_delivery_start_time=row['start_date'],
                    ad_delivery_stop_time=row['end_date'],
                    ad_snapshot_url=row['snapshot_url'],
                    # TODO(macpd): figure out if this is the correct column and how to
                    # transform
                    ad_status=int(row['is_active']),
                    archive_id=row['archive_id'],
                    country_code='US',
                    currency=row['currency'],
                    first_crawl_time=None,
                    funding_entity=row['name'],
                    page_id=row['page_id'],
                    # Below are args required to construct NamedTuple, but not used in ads
                    # table
                    publisher_platform=None,
                    page_name=None,
                    impressions__lower_bound=None,
                    impressions__upper_bound=None,
                    spend__lower_bound=None,
                    spend__upper_bound=None)
                archive_id_to_ad_record[row['archive_id']] = new_record

            self.dest_db_interface.insert_new_ads(
                archive_id_to_ad_record.values())
            self.migrate_impressions_for_archive_id_batch(
                archive_id_to_ad_record)
            num_rows_processed += len(archive_id_to_ad_record)
            logging.info('Migrated %d ad rows so far.', num_rows_processed)
            fetched_rows = src_cursor.fetchmany()

        logging.info('Migrated %d page rows total.', num_rows_processed)

    def migrate_impressions_for_archive_id_batch(self, archive_id_to_ad_record):
        logging.info('Migrationg impressions for archive ID batch.')
        src_cursor = self.get_src_cursor()
        archive_ids = [str(k) for k in archive_id_to_ad_record]
        src_impressions_query = (
            'SELECT * FROM impressions WHERE ad_archive_id IN '
            '(%s)' % (','.join(archive_ids)))
        src_cursor.execute(src_cursor.mogrify(src_impressions_query))

        num_rows_processed = 0
        impression_records = []
        archive_ids_in_results = set()
        fetched_rows = src_cursor.fetchall()
        for row in fetched_rows:
            logging.debug('Processing row: %r', row)
            archive_id = row['ad_archive_id']
            archive_ids_in_results.add(archive_id)
            impression_records.append(
                archive_id_to_ad_record[archive_id]._replace(
                    impressions__lower_bound=row['min_impressions'],
                    impressions__upper_bound=row['max_impressions'],
                    spend__lower_bound=row['min_spend'],
                    spend__upper_bound=row['max_spend']))
            #fetched_rows = src_cursor.fetchall()

        archive_ids_to_fetch = set(archive_id_to_ad_record.keys())
        if archive_ids_in_results != archive_ids_to_fetch:
            logging.error(
                'Did not get impressions for archive IDs: %s. IMPRESSIONS '
                'TABLE MIGRATION MAY BE INCOMPLETE.',
                archive_ids_to_fetch.difference(archive_ids_in_results))

        self.dest_db_interface.insert_new_impressions(impression_records)
        num_rows_processed += len(impression_records)
        logging.info('Migrated %d impression rows for %d archive IDs.',
                     num_rows_processed, len(archive_ids))

    def migrate_demo_impressions_table(self):
        logging.info('Migrating demo_impressions table.')
        src_cursor = self.get_src_cursor()
        src_demo_group_query = 'SELECT * from demo_groups'
        src_cursor.execute(src_cursor.mogrify(src_demo_group_query))
        demo_groups = {}
        gender_list = ['male', 'female', 'unknown']
        for row in src_cursor:
            row_id = row['id']
            if row['gender'] not in gender_list:
                age = row['gender']
                gender = row['age']
                demo_groups[row_id] = (age,gender)
            else: 
                age = row['age']
                gender = row['gender']
                demo_groups[row_id] = (age,gender)

        #handle case with no demo id
        demo_groups[None] = (None, None)

        src_demo_impressions_query = 'SELECT * from demo_impressions'
        src_cursor.execute(src_cursor.mogrify(src_demo_impressions_query))

        commit_every_n_rows = self.batch_size * 100
        num_rows_processed = 0
        fetched_rows = src_cursor.fetchmany()
        while fetched_rows:
            demo_impression_records = []
            for row in fetched_rows:
                demo_impression_records.append(
                        NewSnapshotDemoRecord(
                                archive_id=row['ad_archive_id'],
                                age_range=demo_groups[row['demo_id']][0],
                                gender=demo_groups[row['demo_id']][1],
                                spend_percentage=None,
                                min_impressions=row['min_impressions'],
                                max_impressions=row['max_impressions'],
                                min_spend=row['min_spend'],
                                max_spend=row['max_spend']))

            self.dest_db_interface.insert_new_impression_demos(demo_impression_records)
            num_rows_processed += len(demo_impression_records)
            if num_rows_processed % commit_every_n_rows == 0:
                self.dest_db_connection.commit()
                logging.info("Committing db actions after migrating %d rows", commit_every_n_rows)
            logging.info("Migrated %d demo_impressions rows so far", num_rows_processed)
            fetched_rows = src_cursor.fetchmany()


        logging.info('Migrated %d demo_impression rows total.', num_rows_processed)

    #    src_demo_impressions_query = 'SELECT * from demo_impressions'
    #    src_cursor.execute(src_cursor.mogrify(src_demo_impressions_query))
    #    for row in src_cursor:
    #        demo_impression_records = []
    #        demo_impression_records.append(NewPageRecord(demo_impression_id=row['demo_impression_id'],
    #  demo_impression_name=row['demo_impression_name']))

    #  self.dest_db_interface.insert_new_impression_demos(demo_impression_records)
    #  num_rows_processed += len(demo_impression_records)
    #  logging.info('Migrated %d demo_impression rows so far.', num_rows_processed)

    #  logging.info('Migrated %d demo_impression rows total.', num_rows_processed)

    def migrate_region_impressions_table(self):
        logging.info('Migrating region_impressions table.')
        src_cursor = self.get_src_cursor()
        src_region_query = 'SELECT * from regions'
        src_cursor.execute(src_cursor.mogrify(src_region_query))

        regions = {}
        for row in src_cursor:
            row_id = row['id']
            region = row['name']
            regions[row_id] = region

        src_region_impressions_query = 'SELECT * from region_impressions'
        src_cursor.execute(src_cursor.mogrify(src_region_impressions_query))

        commit_every_n_rows = self.batch_size * 100
        num_rows_processed = 0
        fetched_rows = src_cursor.fetchmany()
        while fetched_rows:
            region_impression_records = []
            for row in fetched_rows:
                region_impression_records.append(NewSnapshotRegionRecord(archive_id=row['ad_archive_id'],
                                                                     region=regions[row['region_id']],
                                                                     spend_percentage=None,
                                                                     min_impressions=row['min_impressions'],
                                                                     max_impressions=row['max_impressions'],
                                                                     min_spend=row['min_spend'],
                                                                     max_spend=row['max_spend']))

            self.dest_db_interface.insert_new_impression_regions(region_impression_records)
            num_rows_processed += len(region_impression_records)
            if num_rows_processed % commit_every_n_rows == 0:
                self.dest_db_connection.commit()
                logging.info("Committing db actions after migrating %d rows", commit_every_n_rows)
            logging.info('Migrated %d region_impression rows so far.', num_rows_processed)
            fetched_rows = src_cursor.fetchmany()

        logging.info('Migrated %d region_impression rows total.', num_rows_processed)

def main(src_db_connection, dest_db_connection, batch_size):
    schema_migrator = SchemaMigrator(src_db_connection, dest_db_connection,
                                     batch_size)
    schema_migrator.run_migration()
    dest_db_connection.commit()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(f"Usage:python3 {sys.argv[0]} schema_migrator.cfg")

    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    logging.basicConfig(
        handlers=[
            logging.FileHandler("schema_migrator.log"),
            logging.StreamHandler()
        ],
        format=
        '[%(levelname)s\t%(asctime)s] {%(pathname)s:%(lineno)d} %(message)s',
        level=logging.INFO)
    src_db_config = config['SRC_POSTGRES']
    src_db_connection = get_db_connection(src_db_config)
    logging.info('Established connection to src database: %s',
                 src_db_connection.dsn)
    dest_db_config = config['DEST_POSTGRES']
    dest_db_connection = get_db_connection(dest_db_config)
    logging.info('Established connection to dest database: %s',
                 dest_db_connection.dsn)
    main(src_db_connection, dest_db_connection, batch_size=2000)
    src_db_connection.close()
    dest_db_connection.close()
