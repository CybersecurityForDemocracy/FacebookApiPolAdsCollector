import collections
import configparser
import datetime
import enum
import hashlib
import logging
import io
import os.path
import re
import sys
import time
import urllib.parse

import dhash
from google.cloud import storage
import requests
from PIL import Image
import psycopg2
import psycopg2.extras
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys

import db_functions
import sim_hash_ad_creative_text

CHROMEDRIVER_PATH='/usr/bin/chromedriver'
GCS_BUCKET = 'facebook_ad_images'
GCS_CREDENTIALS_FILE = 'gcs_credentials.json'
DEFAULT_MAX_ARCHIVE_IDS = 200
DEFAULT_BATCH_SIZE = 20
logging.basicConfig(handlers=[logging.FileHandler("fb_ad_image_retriever.log"),
                              logging.StreamHandler()],
                    format='[%(levelname)s\t%(asctime)s] {%(pathname)s:%(lineno)d} %(message)s',
                    level=logging.DEBUG)

CREATIVE_CONTAINER_XPATH = '//div[@class=\'_7jyg _7jyi\']'
CREATIVE_LINK_CONTAINER_XPATH = (
    CREATIVE_CONTAINER_XPATH + '//a[@class=\'_231w _231z _4yee\']')
CREATIVE_LINK_XPATH_TEMPLATE = (
    CREATIVE_LINK_CONTAINER_XPATH +
    '/div/div/div[%d]/div/div[@class=\'_4ik4 _4ik5\']')
CREATIVE_LINK_TITLE_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 1
CREATIVE_LINK_DESCRIPTION_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 2
CREATIVE_LINK_CAPTION_XPATH = CREATIVE_LINK_XPATH_TEMPLATE % 4
CREATIVE_BODY_CSS_SELECTOR = '_7jyr'
CREATIVE_IMAGE_URL_CSS_SELECTOR = '_7jys'
CREATIVE_IMAGE_URL_XPATH = (
    CREATIVE_CONTAINER_XPATH + '//img[@class=\'_7jys img\']')
MULTIPLE_CREATIVES_VERSION_SLECTOR_ELEMENT_XPATH_TEMPLATE = '//div[@class=\'_a2e\']/div[%d]/div/a'
IMAGE_URL_JSON_NAME = 'original_image_url'
IMAGE_URL_JSON_NULL_PHRASE = '"original_image_url":null'
VIDEO_IMAGE_URL_JSON_NAME = 'video_preview_image_url'
VIDEO_IMAGE_URL_JSON_NULL_PHRASE = '"video_preview_image_url":null'
URL_REGEX_TEMPLATE = r'"%s":\s*?"(http[^"]+?)"'
IMAGE_URL_REGEX = re.compile(URL_REGEX_TEMPLATE % IMAGE_URL_JSON_NAME)
VIDEO_PREVIEW_IMAGE_URL_REGEX = re.compile(URL_REGEX_TEMPLATE % VIDEO_IMAGE_URL_JSON_NAME)
FB_AD_SNAPSHOT_BASE_URL = 'https://www.facebook.com/ads/archive/render_ad/'

class ImageUrlFetchStatus(enum.IntEnum):
  UNKNOWN_ERROR = 0
  SUCCESS = 1
  TIMEOUT = 2
  NOT_FOUND = 3

FetchedAdCreativeData = collections.namedtuple(
    'FetchedAdCreativeData',
    ['archive_id',
     'creative_body',
     'creative_link_url',
     'creative_link_title',
     'creative_link_description',
     'creative_link_caption',
     'image_url'])

AdCreativeRecord = collections.namedtuple('AdCreativeRecord', [
    'archive_id',
    'ad_creative_body',
    'ad_creative_link_url',
    'ad_creative_link_caption',
    'ad_creative_link_title',
    'ad_creative_link_description',
    'text_sha256_hash',
    'image_sha256_hash',
    'snapshot_fetch_time',
    'image_downloaded_url',
    'image_bucket_path',
    'text_sim_hash',
    'image_sim_hash'])

AdImageRecord = collections.namedtuple('AdImageRecord',
    ['archive_id',
     'fetch_time',
     'downloaded_url',
     'bucket_path',
     'image_url_fetch_status',
     'sim_hash',
     'image_sha256'])

def chunks(original_list, chunk_size):
  """Yield successive chunks (of size chunk_size) from original_list."""
  for i in range(0, len(original_list), chunk_size):
    yield original_list[i:i + chunk_size]

def get_headless_chrome_driver(webdriver_executable_path):
  chrome_options = webdriver.ChromeOptions()
  chrome_options.add_argument("--headless")
  driver = webdriver.Chrome(
      executable_path=webdriver_executable_path, chrome_options=chrome_options)
  return driver



def get_database_connection(config):
  host = config['POSTGRES']['HOST']
  dbname = config['POSTGRES']['DBNAME']
  user = config['POSTGRES']['USER']
  password = config['POSTGRES']['PASSWORD']
  port = config['POSTGRES']['PORT']

  db_authorize = "host=%s dbname=%s user=%s password=%s port=%s" % (host, dbname, user, password, port)
  connection = psycopg2.connect(db_authorize)
  logging.info('Established connecton to %s', connection.dsn)
  return connection


def make_gcs_bucket_client(bucket_name, credentials_file):
  storage_client = storage.Client.from_service_account_json(credentials_file)
  bucket_client = storage_client.get_bucket(bucket_name)
  return bucket_client


def construct_snapshot_urls(access_token, archive_ids):
  archive_id_to_snapshot_url = {}
  for archive_id in archive_ids:
    url = '%s?%s' % (FB_AD_SNAPSHOT_BASE_URL, urllib.parse.urlencode({'id': archive_id, 'access_token': access_token}))
    logging.debug('Constructed snapshot URL %s', url)
    archive_id_to_snapshot_url[archive_id] = url
  return archive_id_to_snapshot_url

def get_image_url_list(archive_id, snapshot_url):
  ad_snapshot_request = requests.get(snapshot_url, timeout=30)
  # TODO(macpd): handle this more gracefully
  # TODO(macpd): check encoding
  ad_snapshot_request.raise_for_status()
  ad_snapshot_text = ad_snapshot_request.text

  if VIDEO_IMAGE_URL_JSON_NAME in ad_snapshot_text:
    logging.debug('%s found snapshot. Assuming ad has video with preview image.', VIDEO_IMAGE_URL_JSON_NAME)
    image_url_list = search_for_image_urls_by_regex(VIDEO_PREVIEW_IMAGE_URL_REGEX, ad_snapshot_text)
    if image_url_list:
      return image_url_list

    logging.info('%s found in archive ID %s snapshot, but regex %s did not match.',
        VIDEO_IMAGE_URL_JSON_NAME, archive_id, VIDEO_PREVIEW_IMAGE_URL_REGEX)

  if IMAGE_URL_JSON_NAME in ad_snapshot_text:
    logging.debug('%s found in snapshot. Assuming ad has image.', IMAGE_URL_JSON_NAME)
    image_url_list = search_for_image_urls_by_regex(IMAGE_URL_REGEX, ad_snapshot_text)
    if image_url_list:
      return image_url_list

    logging.info('%s found in archive ID %s snapshot, but regex %s did not match.',
        IMAGE_URL_JSON_NAME, archive_id, IMAGE_URL_REGEX)

  logging.debug('Expected JSON element not found in ad snapshot: ("%s" OR "%s")', IMAGE_URL_JSON_NAME, VIDEO_IMAGE_URL_JSON_NAME)
  # TODO(macpd): raise appropriate error here.
  return None

def search_for_image_urls_by_regex(search_regex, ad_snapshot_text):
  matched_strings = re.findall(search_regex, ad_snapshot_text)
  if not matched_strings:
    logging.debug('Unable to locate image url in ad snapshot using regex: "%s"', search_regex)
    # TODO(macpd): raise appropriate error here.
    return None
  image_url_list = []
  for match in matched_strings:
    raw_image_url_str = match
    logging.debug('Found raw image URL value in ad snapshot: "%s"', raw_image_url_str)
    image_url = raw_image_url_str.replace('\\', '')
    logging.debug('Found image URL value in ad snapshot: "%s"', image_url)
    image_url_list.append(image_url)

  return image_url_list

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


class FacebookAdImageRetriever:

  def __init__(self, db_connection, bucket_client, access_token, batch_size):
    self.bucket_client = bucket_client
    self.num_snapshots_processed = 0
    self.num_snapshots_fetch_failed = 0
    self.num_image_urls_found = 0
    self.num_ids_without_image_url_found = 0
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
        'Image URLs found: %d\n'
        'Archive snapshots without image URL found: %d\n'
        'Images downloads successful: %d\n'
        'Images downloads failed: %d\n'
        'Images uploaded to GCS bucket: %d\n'
        'Average time spent per image: %f seconds',
        self.num_snapshots_processed,
        seconds_elapsed_procesing,
        self.num_snapshots_fetch_failed,
        self.num_image_urls_found,
        self.num_ids_without_image_url_found,
        self.num_image_download_success,
        self.num_image_download_failure,
        self.num_image_uploade_to_gcs_bucket,
        seconds_elapsed_procesing / (self.num_image_uploade_to_gcs_bucket or 1))


  def retreive_and_store_images(self, archive_ids):
    self.start_time = time.monotonic()
    logging.info('Processing %d archive snapshots in batches of %d',
        len(archive_ids), self.batch_size)
    try:
      for archive_id_batch in chunks(archive_ids, self.batch_size):
        self.process_archive_creatives_via_chrome_driver(archive_id_batch)
        self.db_connection.commit()
        logging.info('Processed %d of %d archive snapshots.', self.num_snapshots_processed, len(archive_ids))
        self.log_stats()
    finally:
      self.log_stats()
      self.chromedriver.quit()

  def store_image_in_google_bucket(self, image_dhash, image_bytes):
    image_bucket_path = make_image_hash_file_path(image_dhash)
    blob = self.bucket_client.blob(image_bucket_path)
    blob.upload_from_string(image_bytes)
    self.num_image_uploade_to_gcs_bucket += 1
    logging.debug('Image dhash: %s; uploaded to bucket path: %s', image_dhash, image_bucket_path)
    return image_bucket_path

  def get_video_element_from_creative_container(self, creative_container_element):
    try:
      return creative_container_element.find_element_by_tag_name('video')
    except NoSuchElementException:
      return None

  def get_creative_data_list_via_chromedriver(self, archive_id, snapshot_url):
    logging.info('Getting creatives data from archive ID: %s\nURL: %s',
        archive_id, snapshot_url)
    self.chromedriver.get(snapshot_url)
    creatives = []
    # First find creative text and image as they exist at load time. Then see if
    # there are multiple versions by trying to select the next creative.
    # TODO(macpd): handle videos and extract link, link text, link caption
    for i in range(2, 12):
      creative_container_element = self.chromedriver.find_element_by_xpath(
          CREATIVE_CONTAINER_XPATH)
      creative_body = creative_container_element.find_element_by_class_name(
          CREATIVE_BODY_CSS_SELECTOR).text
      creative_link_url = None
      creative_link_caption = None
      creative_link_title = None
      creative_link_description = None
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
          logging.info(e)

      logging.debug('Found creative text: \'%s\', link_url: \'%s\', link_title: '
                    '\'%s\', lingk_caption: \'%s\', link_description: \'%s\'',
                    creative_body, creative_link_url, creative_link_title,
                    creative_link_caption, creative_link_description)
      video_element = self.get_video_element_from_creative_container(
          creative_container_element)
      if video_element:
        image_url = video_element.get_attribute('poster')
        logging.info('Found <video> tag, assuming creative has video')
      else:
        image_url = creative_container_element.find_element_by_xpath(
            CREATIVE_IMAGE_URL_XPATH).get_attribute('src')
        logging.info('Did not find <video> tag, assuming creative has still image.')
      creatives.append(FetchedAdCreativeData(
          archive_id=archive_id, creative_body=creative_body,
          creative_link_url=creative_link_url,
          creative_link_title=creative_link_title,
          creative_link_description=creative_link_description,
          creative_link_caption=creative_link_caption, image_url=image_url))
      xpath = MULTIPLE_CREATIVES_VERSION_SLECTOR_ELEMENT_XPATH_TEMPLATE % (i)
      try:
        self.chromedriver.find_element_by_xpath(xpath).click()
      except NoSuchElementException:
        break

    return creatives

  def process_archive_creatives_via_chrome_driver(self, archive_id_batch):
    archive_id_to_snapshot_url = construct_snapshot_urls(self.access_token,
        archive_id_batch)
    archive_ids_without_image_url_found = []
    for archive_id, snapshot_url in archive_id_to_snapshot_url.items():
      try:
        creatives = self.get_creative_data_list_via_chromedriver(archive_id,
            snapshot_url)
      except requests.RequestException as request_exception:
        logging.info('Request exception while processing archive id:%s\n%s',
            archive_id, request_exception)
        self.num_snapshots_fetch_failed += 1

      self.num_snapshots_processed += 1
      if creatives:
        logging.debug('Archive ID %s has creative data: %s', archive_id, creatives)
      else:
        logging.info('Unable to find image_url(s) for archive_id: %s, snapshot_url: '
           '%s', archive_id, snapshot_url)
        archive_ids_without_image_url_found.append(archive_id)

    if len(archive_ids_without_image_url_found) == self.batch_size:
      raise RuntimeError('Failed to find image URLs in any snapshot from this '
          'batch.  Assuming access_token has expired. Aborting!')

    self.num_image_urls_found += len(creatives)
    self.num_ids_without_image_url_found += len(archive_ids_without_image_url_found)

    ad_creative_records = []
    for creative in creatives:
      try:
        fetch_time = datetime.datetime.now()
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
      text = creative.creative_body
      # Get simhash as hex without leading '0x'
      text_sim_hash = '%x' % sim_hash_ad_creative_text.hash_ad_creative_text(
              text)
      text_sha256_hash = hashlib.sha256(
              bytes(text, encoding='UTF-32')).hexdigest()
      bucket_path = self.store_image_in_google_bucket(image_dhash, image_bytes)
      ad_creative_records.append(
          AdCreativeRecord(
            ad_creative_body=text,
            ad_creative_link_url=creative.creative_link_url,
            ad_creative_link_caption=creative.creative_link_caption,
            ad_creative_link_title=creative.creative_link_title,
            ad_creative_link_description=creative.creative_link_description,
            archive_id=creative.archive_id,
            snapshot_fetch_time=fetch_time,
            text_sha256_hash=text_sha256_hash,
            text_sim_hash=text_sim_hash,
            image_downloaded_url=creative.image_url,
            image_bucket_path=bucket_path,
            image_sim_hash=image_dhash,
            image_sha256_hash=image_sha256
            ))

    logging.debug('Inserting AdCreativeRecords to DB: %r', ad_creative_records)
    self.db_interface.insert_ad_creative_records(ad_creative_records)

  def process_archive_images(self, archive_id_batch):
    archive_id_to_snapshot_url = construct_snapshot_urls(self.access_token,
        archive_id_batch)
    image_url_to_archive_id = {}
    archive_ids_without_image_url_found = []
    for archive_id, snapshot_url in archive_id_to_snapshot_url.items():
      try:
        image_url_list = get_image_url_list(archive_id, snapshot_url)
      except requests.RequestException as request_exception:
        logging.info('Request exception while processing archive id:%s\n%s',
            archive_id, request_exception)
        self.num_snapshots_fetch_failed += 1

      self.num_snapshots_processed += 1
      if image_url_list:
       for image_url in image_url_list:
         image_url_to_archive_id[image_url] = archive_id

       logging.debug('Archive ID %s has image_url(s): %s', archive_id, image_url_list)
      else:
       logging.info('Unable to find image_url(s) for archive_id: %s, snapshot_url: '
           '%s', archive_id, snapshot_url)
       archive_ids_without_image_url_found.append(archive_id)

    if len(archive_ids_without_image_url_found) == self.batch_size:
      raise RuntimeError('Failed to find image URLs in any snapshot from this '
          'batch.  Assuming access_token has expired. Aborting!')

    self.num_image_urls_found += len(image_url_to_archive_id.keys())
    self.num_ids_without_image_url_found += len(archive_ids_without_image_url_found)

    ad_image_records = []
    for image_url in image_url_to_archive_id:
      try:
        fetch_time = datetime.datetime.now()
        image_request = requests.get(image_url, timeout=30)
        # TODO(macpd): handle this more gracefully
        # TODO(macpd): check encoding
        image_request.raise_for_status()
      except requests.RequestException as request_exception:
        logging.info('Exception %s when requesting image_url: %s', request_exception, image_url)
        self.num_image_download_failure += 1
        # TODO(macpd): handle all error types
        continue

      self.num_image_download_success += 1
      image_bytes = image_request.content
      try:
        image_dhash = get_image_dhash(image_bytes)
      except OSError as error:
        logging.warning(
            "Error generating dhash for archive ID: %s, image_url: %s. "
            "images_bytes len: %d\n%s", image_url_to_archive_id[image_url],
            image_url, len(image_bytes), error)
        continue

      image_sha256 = hashlib.sha256(image_bytes).hexdigest()
      bucket_path = self.store_image_in_google_bucket(image_dhash, image_bytes)
      ad_image_records.append(
          AdImageRecord(
            archive_id=image_url_to_archive_id[image_url],
            fetch_time=fetch_time,
            downloaded_url=image_url,
            bucket_path=bucket_path,
            image_url_fetch_status=int(ImageUrlFetchStatus.SUCCESS),
            sim_hash=image_dhash,
            image_sha256=image_sha256))

    logging.debug('Inserting AdImageRecords to DB: %r', ad_image_records)
    self.db_interface.insert_ad_image_records(ad_image_records)


def main(argv):
  config = configparser.ConfigParser()
  config.read(argv[0])

  access_token = config['FACEBOOK_API']['ACCESS_TOKEN']

  if 'LIMITS' in config and 'BATCH_SIZE' in config['LIMITS']:
    batch_size = int(config['LIMITS']['BATCH_SIZE'])
  else:
    batch_size = DEFAULT_BATCH_SIZE

  if 'LIMITS' in config and 'MAX_ARCHIVE_IDS' in config['LIMITS']:
    max_archive_ids = int(config['LIMITS']['MAX_ARCHIVE_IDS'])
  else:
    max_archive_ids = DEFAULT_MAX_ARCHIVE_IDS

  try:
    with get_database_connection(config) as db_connection:
      logging.info('DB connection established')
      db_interface = db_functions.DBInterface(db_connection)
      if max_archive_ids == -1:
        archive_ids = db_interface.all_archive_ids_without_creatives_data()
      else:
        archive_ids = db_interface.n_archive_ids_without_creatives_data(max_archive_ids)

    with get_database_connection(config) as db_connection:
      bucket_client = make_gcs_bucket_client(GCS_BUCKET, GCS_CREDENTIALS_FILE)
      image_retriever = FacebookAdImageRetriever(db_connection, bucket_client,
          access_token, batch_size)
      image_retriever.retreive_and_store_images(archive_ids)
  finally:
    db_connection.close()

if __name__ == '__main__':
  if len(sys.argv) < 2:
    sys.exit('Usage: %s <config file>' % sys.argv[0])
  main(sys.argv[1:])
