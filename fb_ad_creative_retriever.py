import collections
import concurrent.futures
import configparser
import datetime
import enum
import hashlib
import logging
import io
import os.path
import sys
import time

import dhash
from google.cloud import storage
import requests
from PIL import Image
import psycopg2
import psycopg2.extras
from selenium import webdriver
from selenium.common.exceptions import ElementNotInteractableException, NoSuchElementException, WebDriverException

import db_functions
import sim_hash_ad_creative_text
import standard_logger_config
import snapshot_url_util

CHROMEDRIVER_PATH = '/usr/bin/chromedriver'
GCS_BUCKET = 'facebook_ad_images'
GCS_CREDENTIALS_FILE = 'gcs_credentials.json'
DEFAULT_MAX_ARCHIVE_IDS = 200
DEFAULT_BATCH_SIZE = 20
DEFAULT_BACKOFF_IN_SECONDS = 60
DEFAULT_NUM_THREADS = 2
DEFAULT_NUM_ARCHIVE_IDS_FOR_THREAD = 1000

SNAPSHOT_CONTENT_ROOT_XPATH = '//div[@id=\'content\']'
CREATIVE_CONTAINER_XPATH = '//div[@class=\'_7jyg _7jyi\']'
CREATIVE_LINK_CONTAINER_XPATH = (CREATIVE_CONTAINER_XPATH +
                                 '//a[@class=\'_231w _231z _4yee\']')
CREATIVE_LINK_XPATH_TEMPLATE = (
    CREATIVE_LINK_CONTAINER_XPATH +
    '/div/div/div[%d]/div/div[@class=\'_4ik4 _4ik5\']')
CREATIVE_LINK_TITLE_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 1
CREATIVE_LINK_DESCRIPTION_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 2
CREATIVE_LINK_CAPTION_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 4

EVENT_TYPE_CREATIVE_LINK_XPATH_TEMPLATE = (CREATIVE_LINK_CONTAINER_XPATH +
                                           '/div/div[@class=\'_8jtf\']/div[%d]')
EVENT_TYPE_CREATIVE_LINK_TITLE_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 2
EVENT_TYPE_CREATIVE_LINK_DESCRIPTION_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 3
EVENT_TYPE_CREATIVE_LINK_CAPTION_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 4

CREATIVE_BODY_XPATH = CREATIVE_CONTAINER_XPATH + '/div[@class=\'_7jyr\']'
CREATIVE_IMAGE_URL_XPATH = (CREATIVE_CONTAINER_XPATH +
                            '//img[@class=\'_7jys img\']')
MULTIPLE_CREATIVES_VERSION_SLECTOR_ELEMENT_XPATH_TEMPLATE = (
    '//div[@class=\'_a2e\']/div[%d]/div/a')

CAROUSEL_TYPE_LINK_AND_IMAGE_CONTAINER_XPATH_TEMPLATE = MULTIPLE_CREATIVES_VERSION_SLECTOR_ELEMENT_XPATH_TEMPLATE
CAROUSEL_TYPE_LINK_TITLE_XPATH_TEMPLATE = CREATIVE_LINK_XPATH_TEMPLATE

CAROUSEL_CREATIVE_TYPE_NAVIGATION_ELEM_XPATH = ('//a/div[@class=\'_10sf _5x5_\']')

INVALID_ID_ERROR_TEXT = ("Error: Invalid ID\nPlease ensure that the URL is the same as what's in "
    "the Graph API response.")
AGE_RESTRICTION_ERROR_TEXT = (
        'Because we\'re unable to determine your age, we cannot show you this ad.')

FB_AD_SNAPSHOT_BASE_URL = 'https://www.facebook.com/ads/archive/render_ad/'

AdCreativeLinkAttributes = collections.namedtuple('AdCreativeLinkAttributes', [
    'creative_link_url',
    'creative_link_title',
    'creative_link_description',
    'creative_link_caption'
])

FetchedAdCreativeData = collections.namedtuple('FetchedAdCreativeData', [
    'archive_id',
    'creative_body',
    'creative_link_url',
    'creative_link_title',
    'creative_link_description',
    'creative_link_caption',
    'image_url'
])

AdCreativeRecord = collections.namedtuple('AdCreativeRecord', [
    'archive_id',
    'ad_creative_body',
    'ad_creative_link_url',
    'ad_creative_link_caption',
    'ad_creative_link_title',
    'ad_creative_link_description',
    'text_sha256_hash',
    'image_sha256_hash',
    'image_downloaded_url',
    'image_bucket_path',
    'text_sim_hash',
    'image_sim_hash',
])

AdSnapshotMetadataRecord = collections.namedtuple('AdSnapshotMetadataRecord', [
    'archive_id',
    'snapshot_fetch_time',
    'snapshot_fetch_status'
    ])

AdImageRecord = collections.namedtuple('AdImageRecord', [
    'archive_id',
    'fetch_time',
    'downloaded_url',
    'bucket_path',
    'image_url_fetch_status',
    'sim_hash',
    'image_sha256'
])


class Error(Exception):
    """Generic error type for this module."""


class MaybeBackoffMoreException(Error):
    """Exception to be raised when the retriever probably needs to backoff before resuming."""


class SnapshotNoContentFoundError(Error):
    """Raised if unable to find content in fetched snapshot."""


class SnapshotInvalidIdError(Error):
    """Raised if fetched snapshot has Invalid ID error message."""


class SnapshotAgeRestrictionError(Error):
    """Raised if fetched Snapshot has age restriction error message."""


@enum.unique
class SnapshotFetchStatus(enum.IntEnum):
    UNKNOWN = 0
    SUCCESS = 1
    NO_CONTENT_FOUND = 2
    INVALID_ID_ERROR = 3
    AGE_RESTRICTION_ERROR = 4
    NO_AD_CREATIVES_FOUND = 5


def chunks(original_list, chunk_size):
    """Yield successive chunks (of size chunk_size) from original_list."""
    for i in range(0, len(original_list), chunk_size):
        yield original_list[i:i + chunk_size]


def get_headless_chrome_driver(webdriver_executable_path):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(executable_path=webdriver_executable_path,
                              chrome_options=chrome_options)
    return driver


def get_database_connection(config):
    host = config['POSTGRES']['HOST']
    dbname = config['POSTGRES']['DBNAME']
    user = config['POSTGRES']['USER']
    password = config['POSTGRES']['PASSWORD']
    port = config['POSTGRES']['PORT']

    db_authorize = "host=%s dbname=%s user=%s password=%s port=%s" % (
        host, dbname, user, password, port)
    connection = psycopg2.connect(db_authorize)
    logging.info('Established connecton to %s', connection.dsn)
    return connection


def make_gcs_bucket_client(bucket_name, credentials_file):
    storage_client = storage.Client.from_service_account_json(credentials_file)
    bucket_client = storage_client.get_bucket(bucket_name)
    return bucket_client


def make_image_hash_file_path(image_hash):
    base_file_name = '%s.jpg' % image_hash
    return os.path.join(image_hash[:4], image_hash[4:8], image_hash[8:12],
                        image_hash[12:16], image_hash[16:20], image_hash[20:24],
                        image_hash[24:28], base_file_name)


def get_image_dhash(image_bytes):
    image_file = io.BytesIO(image_bytes)
    image = Image.open(image_file)
    dhash.force_pil()
    row, col = dhash.dhash_row_col(image)
    image_dhash = dhash.format_hex(row, col)
    return image_dhash


class FacebookAdCreativeRetriever:

    def __init__(self, db_connection, bucket_client, access_token, batch_size):
        self.bucket_client = bucket_client
        self.num_snapshots_processed = 0
        self.num_snapshots_fetch_failed = 0
        self.num_ad_creatives_found = 0
        self.num_snapshots_without_creative_found = 0
        self.num_image_download_success = 0
        self.num_image_download_failure = 0
        self.num_image_uploade_to_gcs_bucket = 0
        self.db_connection = db_connection
        self.db_interface = db_functions.DBInterface(db_connection)
        self.access_token = access_token
        self.batch_size = batch_size
        self.start_time = None
        self.chromedriver = get_headless_chrome_driver(CHROMEDRIVER_PATH)

    def get_seconds_elapsed_procesing(self):
        if not self.start_time:
            return 0

        return (time.monotonic() - self.start_time)

    def log_stats(self):
        seconds_elapsed_procesing = self.get_seconds_elapsed_procesing()
        logging.info(
            'Processed %d archive snapshots in %d seconds.\n'
            'Failed to fetch %d archive snapshots.\n'
            'Ad creatives found: %d\n'
            'Archive snapshots without ad creative found: %d\n'
            'Images downloads successful: %d\n'
            'Images downloads failed: %d\n'
            'Images uploaded to GCS bucket: %d\n'
            'Average time spent per ad creative: %f seconds',
            self.num_snapshots_processed, seconds_elapsed_procesing,
            self.num_snapshots_fetch_failed, self.num_ad_creatives_found,
            self.num_snapshots_without_creative_found,
            self.num_image_download_success, self.num_image_download_failure,
            self.num_image_uploade_to_gcs_bucket, seconds_elapsed_procesing /
            (self.num_image_uploade_to_gcs_bucket or 1))

    def retrieve_and_store_ad_creatives(self, archive_ids):
        self.start_time = time.monotonic()
        logging.info('Processing %d archive snapshots in batches of %d',
                     len(archive_ids), self.batch_size)
        try:
            backoff = DEFAULT_BACKOFF_IN_SECONDS
            for archive_id_batch in chunks(archive_ids, self.batch_size):
                try:
                    self.process_archive_creatives_via_chrome_driver(
                        archive_id_batch)
                    self.db_connection.commit()
                    logging.info('Processed %d of %d archive snapshots.',
                                 self.num_snapshots_processed, len(archive_ids))
                    self.log_stats()
                    # if this batch succeeded then reset backoff.
                    backoff = DEFAULT_BACKOFF_IN_SECONDS
                except MaybeBackoffMoreException as e:
                    logging.info(
                        'Was told to chill. Sleeping %d before resuming. error: %s',
                        backoff, e)
                    time.sleep(backoff)
                    if backoff < (10 * backoff):
                        backoff += DEFAULT_BACKOFF_IN_SECONDS

        finally:
            self.log_stats()
            self.chromedriver.quit()

    def store_image_in_google_bucket(self, image_dhash, image_bytes):
        image_bucket_path = make_image_hash_file_path(image_dhash)
        blob = self.bucket_client.blob(image_bucket_path)
        blob.upload_from_string(image_bytes)
        self.num_image_uploade_to_gcs_bucket += 1
        logging.debug('Image dhash: %s; uploaded to bucket path: %s',
                      image_dhash, image_bucket_path)
        return image_bucket_path

    def get_video_element_from_creative_container(self):
        try:
            return self.chromedriver.find_element_by_tag_name('video')
        except NoSuchElementException:
            return None

    def get_image_url_from_creative_container(self):
        try:
            return self.chromedriver.find_element_by_xpath(
                CREATIVE_IMAGE_URL_XPATH).get_attribute('src')
        except NoSuchElementException:
            return None

    def get_event_type_ad_creative_link_attributes(self):
        try:
            creative_link_container = self.chromedriver.find_element_by_xpath(
                CREATIVE_LINK_CONTAINER_XPATH)
            creative_link_url = creative_link_container.get_attribute('href')
            creative_link_title = self.chromedriver.find_element_by_xpath(
                EVENT_TYPE_CREATIVE_LINK_TITLE_XPATH).text
            creative_link_caption = self.chromedriver.find_element_by_xpath(
                EVENT_TYPE_CREATIVE_LINK_CAPTION_XPATH).text
            creative_link_description = self.chromedriver.find_element_by_xpath(
                EVENT_TYPE_CREATIVE_LINK_DESCRIPTION_XPATH).text
        except NoSuchElementException as e:
            return AdCreativeLinkAttributes(creative_link_url=None,
                                            creative_link_caption=None,
                                            creative_link_title=None,
                                            creative_link_description=None)

        return AdCreativeLinkAttributes(
            creative_link_url=creative_link_url,
            creative_link_caption=creative_link_caption,
            creative_link_title=creative_link_title,
            creative_link_description=creative_link_description)

    def get_ad_creative_link_attributes(self):
        try:
            creative_link_container = self.chromedriver.find_element_by_xpath(
                CREATIVE_LINK_CONTAINER_XPATH)
            creative_link_url = creative_link_container.get_attribute('href')
            creative_link_title = self.chromedriver.find_element_by_xpath(
                CREATIVE_LINK_TITLE_XPATH).text
            creative_link_caption = self.chromedriver.find_element_by_xpath(
                CREATIVE_LINK_CAPTION_XPATH).text
            creative_link_description = self.chromedriver.find_element_by_xpath(
                CREATIVE_LINK_DESCRIPTION_XPATH).text
        except NoSuchElementException as e:
            return None

        return AdCreativeLinkAttributes(
            creative_link_url=creative_link_url,
            creative_link_caption=creative_link_caption,
            creative_link_title=creative_link_title,
            creative_link_description=creative_link_description)

    def ad_snapshot_has_carousel_navigation_element(self):
        try:
            self.chromedriver.find_element_by_xpath(CAROUSEL_CREATIVE_TYPE_NAVIGATION_ELEM_XPATH)
        except NoSuchElementException:
            return False

        return True

    def get_ad_creative_body(self, archive_id):
        creative_body = None
        try:
            creative_container_element = self.chromedriver.find_element_by_xpath(
                CREATIVE_CONTAINER_XPATH)
            creative_body = creative_container_element.find_element_by_xpath(
                CREATIVE_BODY_XPATH).text
        except NoSuchElementException as e:
            logging.info(
                'Unable to find ad creative body section for Archive ID: %s, Ad appers to have NO '
                'creative(s). \nError: %s', archive_id, e)

        return creative_body

    def get_ad_creative_carousel_link_attributes(self, carousel_index, archive_id, creative_body):
        try:
            xpath = CAROUSEL_TYPE_LINK_TITLE_XPATH_TEMPLATE % carousel_index
            creative_link_container = self.chromedriver.find_element_by_xpath(xpath)
            creative_link_url = creative_link_container.get_attribute('href')
            creative_link_title = creative_link_container.text
            xpath = '%s/img' % xpath
            image_url = self.chromedriver.find_element_by_xpath(xpath).get_attribute('src')
        except NoSuchElementException as e:
            return None

        return FetchedAdCreativeData(
            achive_id=archive_id,
            creative_body=creative_body,
            creative_link_url=creative_link_url,
            creative_link_title=creative_link_title,
            creative_link_caption=None,
            creative_link_description=None,
            image_url=image_url)

    def get_carousel_ad_creative_data(self, archive_id):
        fetched_ad_creatives = []
        creative_body = self.get_ad_creative_body(archive_id)
        for carousel_index in range(1, 10):
            fetched_ad_creative_data = self.get_ad_creative_carousel_link_attributes(
                carousel_index, archive_id, creative_body)
            if fetched_ad_creative_data:
                fetched_ad_creatives.append(fetched_ad_creative_data)
            else:
                break
        return fetched_ad_creatives


    def get_displayed_ad_creative_data(self, archive_id):
        creative_body = self.get_ad_creative_body(archive_id)

        try:
            self.chromedriver.find_element_by_xpath(
                CREATIVE_LINK_CONTAINER_XPATH)
        except NoSuchElementException as e:
            logging.info(
                'Unable to find ad creative link section for Archive ID: %s. '
                '\nError: %s', archive_id, e)


        link_attrs = self.get_ad_creative_link_attributes()
        if not link_attrs:
            # If ad creative link attributes aren't found, parse page as an event
            # type ad.
            link_attrs = self.get_event_type_ad_creative_link_attributes()

        logging.debug(
            'Found creative text: \'%s\', link_url: \'%s\', link_title: '
            '\'%s\', lingk_caption: \'%s\', link_description: \'%s\'',
            creative_body, link_attrs.creative_link_url,
            link_attrs.creative_link_title, link_attrs.creative_link_caption,
            link_attrs.creative_link_description)

        # TODO(macpd): handle image carousels, and "invalid IDs"
        video_element = self.get_video_element_from_creative_container()
        if video_element:
            image_url = video_element.get_attribute('poster')
            logging.debug('Found <video> tag, assuming creative has video')
        else:
            image_url = self.get_image_url_from_creative_container()
            if image_url:
                logging.debug('Did not find <video> tag, but found <img> src')
            else:
                logging.info(
                    'Found neither <video> nor <img> tag. Assuming no video '
                    'or image in ad creative.')

        return FetchedAdCreativeData(
            archive_id=archive_id,
            creative_body=creative_body,
            creative_link_url=link_attrs.creative_link_url,
            creative_link_title=link_attrs.creative_link_title,
            creative_link_description=link_attrs.creative_link_description,
            creative_link_caption=link_attrs.creative_link_caption,
            image_url=image_url)

    def raise_if_page_has_age_restriction_or_id_error(self):
        error_text = None
        try:
            error_text = self.chromedriver.find_element_by_xpath(SNAPSHOT_CONTENT_ROOT_XPATH).text
        except NoSuchElementException:
            raise SnapshotNoContentFoundError

        if INVALID_ID_ERROR_TEXT in error_text:
            raise SnapshotInvalidIdError

        if AGE_RESTRICTION_ERROR_TEXT in error_text:
            raise SnapshotAgeRestrictionError


    def get_creative_data_list_via_chromedriver_with_retry_on_driver_error(self, archive_id,
                                                                           snapshot_url):
        """Attempts to get ad creative(s) data. Restarts webdriver and retries if error raised."""
        try:
            return self.get_creative_data_list_via_chromedriver(archive_id, snapshot_url)
        except WebDriverException as chromedriver_exception:
            logging.info('Chromedriver exception %s.\nRestarting chromedriver.',
                         chromedriver_exception)
            self.chromedriver.quit()
            self.chromedriver = get_headless_chrome_driver(CHROMEDRIVER_PATH)

        return self.get_creative_data_list_via_chromedriver(archive_id, snapshot_url)


    def get_creative_data_list_via_chromedriver(self, archive_id, snapshot_url):
        logging.info('Getting creatives data from archive ID: %s\nURL: %s',
                     archive_id, snapshot_url)
        self.chromedriver.get(snapshot_url)

        self.raise_if_page_has_age_restriction_or_id_error()

        # If ad has carousel, it should not have multiple versions. Instead it will have multiple
        # images with different images and links.
        if self.ad_snapshot_has_carousel_navigation_element():
            fetched_ad_creative_data_list = self.get_carousel_ad_creative_data(archive_id)
            if fetched_ad_creative_data_list:
                return fetched_ad_creative_data_list


        # If ad does not have carousel navigation elements, or no ad creatives data was found when
        # parsed as a caroursel type, ad likely has one image/body per version.
        creatives = []
        for i in range(2, 11):
            fetched_ad_creative_data = self.get_displayed_ad_creative_data(
                archive_id)
            if fetched_ad_creative_data:
                creatives.append(fetched_ad_creative_data)
            else:
                # creative index is i-1 because loop first looks at current creative and then
                # attempts to interact with page to select next creative.
                logging.warning(
                    'No ad creative data for archive ID: %d creative index %d',
                    archive_id, i - 1)
                break

            # Attempt to select next ad creative version. If no such element, assume
            # only one creative version is available.
            # TODO(macpd): Figure out why, and properly handle,
            # ElementNotInteractableException in this context.
            xpath = MULTIPLE_CREATIVES_VERSION_SLECTOR_ELEMENT_XPATH_TEMPLATE % (
                i)
            try:
                self.chromedriver.find_element_by_xpath(xpath).click()
            except ElementNotInteractableException as elem_error:
                logging.warning(
                    'Element to select from multiple creatives appears to '
                    'be present at xpath \'%s\', but is not interactable. '
                    'Archive ID: %s.\nerror: %s', xpath, archive_id, elem_error)
                break
            except NoSuchElementException:
                break

        return creatives

    def process_archive_creatives_via_chrome_driver(self, archive_id_batch):
        archive_id_to_snapshot_url = snapshot_url_util.construct_archive_id_to_snapshot_url_map(
            self.access_token, archive_id_batch)
        archive_ids_without_creative_found = []
        creatives = []
        snapshot_metadata_records = []
        for archive_id, snapshot_url in archive_id_to_snapshot_url.items():
            snapshot_fetch_status = SnapshotFetchStatus.UNKNOWN
            try:
                fetch_time = datetime.datetime.now()
                new_creatives = self.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                        archive_id, snapshot_url)
                if new_creatives:
                    creatives.extend(new_creatives)
                    snapshot_fetch_status = SnapshotFetchStatus.SUCCESS
                else:
                    archive_ids_without_creative_found.append(archive_id)
                    snapshot_fetch_status = SnapshotFetchStatus.NO_AD_CREATIVES_FOUND
                    logging.info(
                        'Unable to find ad creative(s) for archive_id: %s, snapshot_url: '
                        '%s', archive_id, snapshot_url)

            except requests.RequestException as request_exception:
                logging.info(
                    'Request exception while processing archive id:%s\n%s',
                    archive_id, request_exception)
                self.num_snapshots_fetch_failed += 1
                # TODO(macpd): decide how to count the errors below
            except SnapshotNoContentFoundError as error:
                logging.info('No content found for archive_id %d, %s', archive_id, snapshot_url)
                snapshot_fetch_status = SnapshotFetchStatus.NO_CONTENT_FOUND
            except SnapshotAgeRestrictionError as error:
                snapshot_fetch_status = SnapshotFetchStatus.AGE_RESTRICTION_ERROR
            except SnapshotInvalidIdError as error:
                snapshot_fetch_status = SnapshotFetchStatus.INVALID_ID_ERROR

            snapshot_metadata_records.append(AdSnapshotMetadataRecord(
                    archive_id=archive_id, snapshot_fetch_time=fetch_time,
                    snapshot_fetch_status=snapshot_fetch_status))
            self.num_snapshots_processed += 1

        if len(archive_ids_without_creative_found) == self.batch_size:
            raise MaybeBackoffMoreException(
                'Failed to find ad creatives in any snapshot from this '
                'batch.  Assuming access_token has expired. Aborting!')

        self.num_ad_creatives_found += len(creatives)
        self.num_snapshots_without_creative_found += len(
            archive_ids_without_creative_found)

        ad_creative_records = []
        for creative in creatives:
            image_dhash = None
            image_sha256 = None
            bucket_path = None
            fetch_time = datetime.datetime.now()
            if creative.image_url:
                try:
                    image_request = requests.get(creative.image_url, timeout=30)
                    # TODO(macpd): handle this more gracefully
                    # TODO(macpd): check encoding
                    image_request.raise_for_status()
                except requests.RequestException as request_exception:
                    logging.info('Exception %s when requesting image_url: %s',
                                 request_exception, creative.image_url)
                    self.num_image_download_failure += 1
                    # TODO(macpd): handle all error types
                    continue

                image_bytes = image_request.content
                try:
                    image_dhash = get_image_dhash(image_bytes)
                except OSError as error:
                    logging.warning(
                        "Error generating dhash for archive ID: %s, image_url: %s. "
                        "images_bytes len: %d\n%s", creative.archive_id,
                        creative.image_url, len(image_bytes), error)
                    self.num_image_download_failure += 1
                    continue

                self.num_image_download_success += 1
                image_sha256 = hashlib.sha256(image_bytes).hexdigest()
                bucket_path = self.store_image_in_google_bucket(
                    image_dhash, image_bytes)

            text = None
            text_sim_hash = None
            text_sha256_hash = None
            if creative.creative_body:
                text = creative.creative_body
                # Get simhash as hex without leading '0x'
                text_sim_hash = '%x' % sim_hash_ad_creative_text.hash_ad_creative_text(
                    text)
                text_sha256_hash = hashlib.sha256(bytes(
                    text, encoding='UTF-32')).hexdigest()

            ad_creative_records.append(
                AdCreativeRecord(
                    ad_creative_body=text,
                    ad_creative_link_url=creative.creative_link_url,
                    ad_creative_link_caption=creative.creative_link_caption,
                    ad_creative_link_title=creative.creative_link_title,
                    ad_creative_link_description=creative.creative_link_description,
                    archive_id=creative.archive_id,
                    text_sha256_hash=text_sha256_hash,
                    text_sim_hash=text_sim_hash,
                    image_downloaded_url=creative.image_url,
                    image_bucket_path=bucket_path,
                    image_sim_hash=image_dhash,
                    image_sha256_hash=image_sha256))

        logging.info('Inserting %d AdCreativeRecords to to DB.',
                     len(ad_creative_records))
        logging.debug('Inserting AdCreativeRecords to DB: %r',
                      ad_creative_records)
        self.db_interface.insert_ad_creative_records(ad_creative_records)
        logging.info('Updating %d snapshot metadata records.', len(snapshot_metadata_records))
        self.db_interface.update_ad_snapshot_metadata(snapshot_metadata_records)


def retrieve_and_store_ad_creatives(config, access_token, archive_ids, batch_size):
    with get_database_connection(config) as db_connection:
        bucket_client = make_gcs_bucket_client(GCS_BUCKET, GCS_CREDENTIALS_FILE)
        image_retriever = FacebookAdCreativeRetriever(
            db_connection, bucket_client, access_token, batch_size)
        image_retriever.retrieve_and_store_ad_creatives(archive_ids)

def main(argv):
    config = configparser.ConfigParser()
    config.read(argv[0])

    access_token = config['FACEBOOK_API']['ACCESS_TOKEN']

    if 'LIMITS' in config and 'BATCH_SIZE' in config['LIMITS']:
        batch_size = int(config['LIMITS']['BATCH_SIZE'])
    else:
        batch_size = DEFAULT_BATCH_SIZE
    logging.info('Batch size: %d', batch_size)

    if 'LIMITS' in config and 'MAX_ARCHIVE_IDS' in config['LIMITS']:
        max_archive_ids = int(config['LIMITS']['MAX_ARCHIVE_IDS'])
    else:
        max_archive_ids = DEFAULT_MAX_ARCHIVE_IDS

    logging.info('Max archive IDs to process: %d', max_archive_ids)

    max_threads = int(config.get('LIMITS', {}).get('MAX_THREADS', DEFAULT_NUM_THREADS))
    logging.info('Max threads: %d', max_threads)

    with get_database_connection(config) as db_connection:
        logging.info('DB connection established')
        db_interface = db_functions.DBInterface(db_connection)
        if max_archive_ids == -1:
            archive_ids = db_interface.all_archive_ids_that_need_scrape()
        else:
            archive_ids = db_interface.n_archive_ids_that_need_scrape(max_archive_ids)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for archive_id_batch in chunks(archive_ids, DEFAULT_NUM_ARCHIVE_IDS_FOR_THREAD):
            futures.append(executor.submit(
                    retrieve_and_store_ad_creatives, config, access_token, archive_id_batch,
                    batch_size))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    standard_logger_config.configure_logger("fb_ad_creative_retriever.log")
    main(sys.argv[1:])
