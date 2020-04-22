import collections
import configparser
import datetime
import enum
import hashlib
import logging
import io
import os.path
import socket
import sys
import time

import dhash
from google.cloud import storage
import requests
from PIL import Image
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (ElementNotInteractableException, NoSuchElementException,
    WebDriverException, ElementClickInterceptedException)
import tenacity

import config_utils
import db_functions
import sim_hash_ad_creative_text
import slack_notifier
import snapshot_url_util

LOGGER = logging.getLogger(__name__)
CHROMEDRIVER_PATH = '/usr/bin/chromedriver'
CHROME_BROWSER_PATH = '/usr/bin/chromium-browser'
AD_CREATIVE_IMAGES_BUCKET = 'facebook_ad_images'
ARCHIVE_SCREENSHOTS_BUCKET = 'facebook_ad_archive_screenshots'
GCS_CREDENTIALS_FILE = 'gcs_credentials.json'
DEFAULT_MAX_ARCHIVE_IDS = 200
DEFAULT_BATCH_SIZE = 20
DEFAULT_BACKOFF_IN_SECONDS = 60
RESET_CHROME_DRIVER_AFTER_PROCESSING_N_SNAPSHOTS = 2000
TOO_MANY_REQUESTS_SLEEP_TIME = 4 * 60 * 60 # 4 hours

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
# Arrow elemnt to navigate multiple creative selection UI that is too large to fit in UI bounding
# box. (ex: 411302822856762).
MULTIPLE_CREATIVES_OVERFLOW_NAVIGATION_ELEMENT_XPATH = (
    SNAPSHOT_CONTENT_ROOT_XPATH + '/div/div[2]/div/div/div/div[2]/div[2]/div/a')

CAROUSEL_TYPE_LINK_AND_IMAGE_CONTAINER_XPATH_TEMPLATE = MULTIPLE_CREATIVES_VERSION_SLECTOR_ELEMENT_XPATH_TEMPLATE
CAROUSEL_TYPE_LINK_TITLE_XPATH_TEMPLATE = CREATIVE_LINK_XPATH_TEMPLATE

CAROUSEL_CREATIVE_TYPE_NAVIGATION_ELEM_XPATH = ('//a/div[@class=\'_10sf _5x5_\']')

INVALID_ID_ERROR_TEXT = ("Error: Invalid ID\nPlease ensure that the URL is the same as what's in "
    "the Graph API response.")
AGE_RESTRICTION_ERROR_TEXT = (
        'Because we\'re unable to determine your age, we cannot show you this ad.')
TOO_MANY_REQUESTS_ERROR_TEXT = (
    'You have been temporarily blocked from searching or viewing the Ad Library due to too many '
    'requests. Please try again later.').lower()

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
    'image_url',
])

AdScreenshotAndCreatives = collections.namedtuple('AdScreenshotAndCreatives', [
    'screenshot_binary_data',
    'creatives'
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

class TooManyRequestsException(Error):
    """Exception to be raised when the retriever is told it has made too many requests too quickly.
    """


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
    chrome_options = Options()
    chrome_options.binary_location = CHROME_BROWSER_PATH
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    prefs = {"profile.default_content_setting_values.notifications":2,
             "profile.managed_default_content_settings.stylesheets":2,
             "profile.managed_default_content_settings.cookies":2,
             "profile.managed_default_content_settings.javascript":1,
             "profile.managed_default_content_settings.plugins":1,
             "profile.managed_default_content_settings.popups":2,
             "profile.managed_default_content_settings.geolocation":2,
             "profile.managed_default_content_settings.media_stream":2}
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(executable_path=webdriver_executable_path, options=chrome_options)
    driver.set_window_size(800, 1000)
    return driver


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

@tenacity.retry(stop=tenacity.stop_after_attempt(4),
                wait=tenacity.wait_random_exponential(multiplier=1, max=30),
                before_sleep=tenacity.before_sleep_log(LOGGER, logging.INFO))
def upload_blob(bucket_client, blob_path, blob_data):
    blob = bucket_client.blob(blob_path)
    blob.upload_from_string(blob_data)
    return blob.id


class FacebookAdCreativeRetriever:

    def __init__(self, db_connection, ad_creative_images_bucket_client,
                 archive_screenshots_bucket_client, access_token, commit_to_db_every_n_processed,
                 slack_url):
        self.ad_creative_images_bucket_client = ad_creative_images_bucket_client
        self.archive_screenshots_bucket_client = archive_screenshots_bucket_client
        self.num_snapshots_processed = 0
        self.num_snapshots_fetch_failed = 0
        self.num_ad_creatives_found = 0
        self.num_snapshots_without_creative_found = 0
        self.num_image_download_success = 0
        self.num_image_download_failure = 0
        self.num_image_uploade_to_gcs_bucket = 0
        self.current_batch_id = None
        self.db_connection = db_connection
        self.db_interface = db_functions.DBInterface(db_connection)
        self.access_token = access_token
        self.commit_to_db_every_n_processed = commit_to_db_every_n_processed
        self.start_time = None
        self.slack_url = slack_url
        self.chromedriver = get_headless_chrome_driver(CHROMEDRIVER_PATH)

    def get_seconds_elapsed_procesing(self):
        if not self.start_time:
            return 0

        return (time.monotonic() - self.start_time)

    def reset_chromedriver(self):
        logging.info('Resetting chromedriver.')
        self.chromedriver.quit()
        self.chromedriver = get_headless_chrome_driver(CHROMEDRIVER_PATH)

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
            'Average time spent per ad creative: %f seconds\n'
            'Current batch ID: %s',
            self.num_snapshots_processed, seconds_elapsed_procesing,
            self.num_snapshots_fetch_failed, self.num_ad_creatives_found,
            self.num_snapshots_without_creative_found,
            self.num_image_download_success, self.num_image_download_failure,
            self.num_image_uploade_to_gcs_bucket, seconds_elapsed_procesing /
            (self.num_image_uploade_to_gcs_bucket or 1),
            self.current_batch_id)


    def retreive_and_store_ad_creatives(self):
        self.start_time = time.monotonic()
        try:
            num_snapshots_processed_since_chromedriver_reset = 0
            batch_and_archive_ids = self.db_interface.get_archive_id_batch_to_fetch()
            while batch_and_archive_ids:
                self.current_batch_id = batch_and_archive_ids['batch_id']
                archive_ids = batch_and_archive_ids['archive_ids']
                logging.info('Processing batch ID %d of %d archive snapshots in chunks of %d',
                             self.current_batch_id, len(archive_ids),
                             self.commit_to_db_every_n_processed)
                self.process_archive_id_batch(archive_ids)
                num_snapshots_processed_since_chromedriver_reset += len(archive_ids)
                if (num_snapshots_processed_since_chromedriver_reset >=
                        RESET_CHROME_DRIVER_AFTER_PROCESSING_N_SNAPSHOTS):
                    logging.info('Processed %d snapshots since last reset (limit: %d)',
                                 num_snapshots_processed_since_chromedriver_reset,
                                 RESET_CHROME_DRIVER_AFTER_PROCESSING_N_SNAPSHOTS)
                    self.reset_chromedriver()
                    num_snapshots_processed_since_chromedriver_reset = 0

                self.db_interface.mark_fetch_batch_completed(self.current_batch_id)
                self.db_connection.commit()
                batch_and_archive_ids = self.db_interface.get_archive_id_batch_to_fetch()
        finally:
            self.log_stats()
            self.chromedriver.quit()

    def process_archive_id_batch(self, archive_id_batch):
        num_snapshots_processed_in_current_batch = 0
        for archive_id_chunk in chunks(archive_id_batch, self.commit_to_db_every_n_processed):
            self.process_archive_creatives_via_chrome_driver(
                archive_id_chunk)
            self.db_connection.commit()
            num_snapshots_processed_in_current_batch += len(archive_id_chunk)
            logging.info('Processed %d of %d archive snapshots.',
                         num_snapshots_processed_in_current_batch, len(archive_id_batch))
            self.log_stats()

    def store_image_in_google_bucket(self, image_dhash, image_bytes):
        image_bucket_path = make_image_hash_file_path(image_dhash)
        blob_id = upload_blob(self.ad_creative_images_bucket_client, image_bucket_path, image_bytes)
        self.num_image_uploade_to_gcs_bucket += 1
        logging.debug('Image dhash: %s; uploaded to: %s', image_dhash, blob_id)
        return image_bucket_path

    def store_snapshot_screenshot(self, archive_id, screenshot_binary_data):
        bucket_path = '%d.png' % archive_id
        blob_id = upload_blob(self.archive_screenshots_bucket_client, bucket_path,
                              screenshot_binary_data)
        logging.debug('Uploaded %d archive_id snapshot to %s', archive_id, blob_id)

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

    def get_ad_creative_container_element(self, archive_id):
        try:
            return self.chromedriver.find_element_by_xpath(CREATIVE_CONTAINER_XPATH)
        except NoSuchElementException as e:
            logging.info(
                'Unable to find ad creative container for Archive ID: %s, Ad appers to have NO '
                'creative(s). \nError: %s', archive_id, e)
        return None

    def get_ad_creative_body(self, archive_id):
        creative_body = None
        try:
            creative_container_element = self.get_ad_creative_container_element(archive_id)
            if not creative_container_element:
                return None

            creative_body = creative_container_element.find_element_by_xpath(
                CREATIVE_BODY_XPATH).text
        except NoSuchElementException as e:
            logging.info(
                'Unable to find ad creative body section for Archive ID: %s, Ad appers to have NO '
                'creative(s). \nError: %s', archive_id, e)

        return creative_body

    def get_ad_snapshot_screenshot(self, archive_id):
        creative_container_element = self.get_ad_creative_container_element(archive_id)
        if creative_container_element:
            return creative_container_element.screenshot_as_png

        logging.info('Unable to get creative container for screenshot of archive ID %d.',
                     archive_id)
        return None

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
            page_text = self.chromedriver.find_element_by_tag_name('html').text
            logging.warning('Could not find content in page:%s', page_text)
            if TOO_MANY_REQUESTS_ERROR_TEXT in page_text.lower():
                raise TooManyRequestsException

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
            self.reset_chromedriver()

        return self.get_creative_data_list_via_chromedriver(archive_id, snapshot_url)

    def click_multiple_creative_overflow_navigation_arrow(self):
        try:
            navigation_elem = self.chromedriver.find_element_by_xpath(
                    MULTIPLE_CREATIVES_OVERFLOW_NAVIGATION_ELEMENT_XPATH)
            navigation_elem.click()
        except NoSuchElementException:
            return False
        return True


    def get_creative_data_list_via_chromedriver(self, archive_id, snapshot_url):
        logging.info('Getting creatives data from archive ID: %s', archive_id)
        logging.debug('Getting creatives data from archive ID: %s\nURL: %s',
                     archive_id, snapshot_url)
        self.chromedriver.get(snapshot_url)

        self.raise_if_page_has_age_restriction_or_id_error()
        screenshot = self.get_ad_snapshot_screenshot(archive_id)

        # If ad has carousel, it should not have multiple versions. Instead it will have multiple
        # images with different images and links.
        if self.ad_snapshot_has_carousel_navigation_element():
            fetched_ad_creative_data_list = self.get_carousel_ad_creative_data(archive_id)
            if fetched_ad_creative_data_list:
                return AdScreenshotAndCreatives(screenshot_binary_data=screenshot,
                                                creatives=fetched_ad_creative_data_list)


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
            except ElementClickInterceptedException:
                # If there are more ad creatives than can fit in the multiple creative selection
                # list the UI renders a navitation arrow over the last visible element. That arrow
                # element intercepts clicks. So we click the element to move the next ad creative
                # selection element into a clickable position.
                self.click_multiple_creative_overflow_navigation_arrow()

                # Sometimes after chrome is reset FB ad snapshot UI will show an informational diaglog
                # that occludes the multiple creative selection elements.
                # First we click on the previous element in the multiple creative selector to move
                # focus elsewhere and dismiss the diaglog
                xpath = MULTIPLE_CREATIVES_VERSION_SLECTOR_ELEMENT_XPATH_TEMPLATE % (
                    i - 1)
                self.chromedriver.find_element_by_xpath(xpath).click()
                # Then click on the desired element.
                xpath = MULTIPLE_CREATIVES_VERSION_SLECTOR_ELEMENT_XPATH_TEMPLATE % (
                    i)
                self.chromedriver.find_element_by_xpath(xpath).click()
            except ElementNotInteractableException as elem_error:
                logging.warning(
                    'Element to select from multiple creatives appears to '
                    'be present at xpath \'%s\', but is not interactable. '
                    'Archive ID: %s.\nerror: %s', xpath, archive_id, elem_error)
                break
            except NoSuchElementException:
                break

        return AdScreenshotAndCreatives(screenshot_binary_data=screenshot, creatives=creatives)

    def process_archive_creatives_via_chrome_driver(self, archive_id_batch):
        archive_id_to_snapshot_url = snapshot_url_util.construct_archive_id_to_snapshot_url_map(
            self.access_token, archive_id_batch)
        archive_ids_without_creative_found = []
        creatives = []
        snapshot_metadata_records = []
        archive_id_to_screenshot = {}
        for archive_id, snapshot_url in archive_id_to_snapshot_url.items():
            snapshot_fetch_status = SnapshotFetchStatus.UNKNOWN
            try:
                fetch_time = datetime.datetime.now()
                screenshot_and_creatives = (
                    self.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                        archive_id, snapshot_url))
                if screenshot_and_creatives.screenshot_binary_data:
                    archive_id_to_screenshot[archive_id] = (
                        screenshot_and_creatives.screenshot_binary_data)
                if screenshot_and_creatives.creatives:
                    creatives.extend(screenshot_and_creatives.creatives)
                    snapshot_fetch_status = SnapshotFetchStatus.SUCCESS
                else:
                    archive_ids_without_creative_found.append(archive_id)
                    snapshot_fetch_status = SnapshotFetchStatus.NO_AD_CREATIVES_FOUND
                    logging.info(
                        'Unable to find ad creative(s) for archive_id: %s, snapshot_url: '
                        '%s', archive_id, snapshot_url)

            except TooManyRequestsException as error:
                slack_msg = (
                    ':rotating_light: :rotating_light: :rotating_light: '
                    'fb_ad_creative_retriever.py thread raised with TooManyRequestsException on '
                    'host %s. Sleeping %d seconds! :rotating_light: :rotating_light: '
                    ':rotating_light:' % (socket.getfqdn(), TOO_MANY_REQUESTS_SLEEP_TIME))
                slack_notifier.notify_slack(self.slack_url, slack_msg)
                logging.error('TooManyRequestsException raised. Sleeping %d seconds.',
                              TOO_MANY_REQUESTS_SLEEP_TIME)
                time.sleep(TOO_MANY_REQUESTS_SLEEP_TIME)

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

        self.num_ad_creatives_found += len(creatives)
        self.num_snapshots_without_creative_found += len(
            archive_ids_without_creative_found)

        for archive_id in archive_id_to_screenshot:
            self.store_snapshot_screenshot(archive_id, archive_id_to_screenshot[archive_id])

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


def main(argv):
    config = configparser.ConfigParser()
    config.read(argv[0])

    access_token = config_utils.get_facebook_access_token(config)
    commit_to_db_every_n_processed = config.getint('LIMITS', 'BATCH_SIZE', fallback=DEFAULT_BATCH_SIZE)
    logging.info('Will commit to DB every %d snapshots processed.', commit_to_db_every_n_processed)
    slack_url = config.get('LOGGING', 'SLACK_URL')

    database_connection_params = config_utils.get_database_connection_params_from_config(config)

    with config_utils.get_database_connection(database_connection_params) as db_connection:
        ad_creative_images_bucket_client = make_gcs_bucket_client(AD_CREATIVE_IMAGES_BUCKET,
                                                                  GCS_CREDENTIALS_FILE)
        archive_screenshots_bucket_client = make_gcs_bucket_client(ARCHIVE_SCREENSHOTS_BUCKET,
                                                                  GCS_CREDENTIALS_FILE)
        image_retriever = FacebookAdCreativeRetriever(
            db_connection, ad_creative_images_bucket_client, archive_screenshots_bucket_client,
            access_token, commit_to_db_every_n_processed, slack_url)
        image_retriever.retreive_and_store_ad_creatives()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    config_utils.configure_logger("fb_ad_creative_retriever.log")
    main(sys.argv[1:])
