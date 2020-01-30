import collections
import configparser
import datetime
import dhash
import enum
from google.cloud import storage
import hashlib
import io
import json
import logging
import os.path
import re
import requests
from PIL import Image
import psycopg2
import psycopg2.extras
import sys
import urllib.parse

import db_functions

GCS_BUCKET = 'facebook_ad_images'
GCS_CREDENTIALS_FILE = 'gcs_credentials.json'
DEFAULT_MAX_ARCHIVE_IDS = 200
DEFAULT_BATCH_SIZE = 20
logging.basicConfig(handlers=[logging.FileHandler("fb_ad_image_retriever.log"),
                              logging.StreamHandler()],
                    format='[%(levelname)s\t%(asctime)s] {%(pathname)s:%(lineno)d} %(message)s',
                    level=logging.INFO)

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

AdImageRecord = collections.namedtuple('AdImageRecord',
    ['archive_id',
     'fetch_time',
     'downloaded_url',
     'bucket_url',
     'image_url_fetch_status',
     'sim_hash',
     'image_sha256'])

def chunks(lst, n):
  """Yield successive n-sized chunks from lst."""
  for i in range(0, len(lst), n):
    yield lst[i:i + n]


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

  logging.warning('Expected JSON element not found in ad snapshot: ("%s" OR "%s")', IMAGE_URL_JSON_NAME, VIDEO_IMAGE_URL_JSON_NAME)
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

  def log_stats(self):
    logging.info(
        'Processed %d archive snapshots.\n'
        'Failed to fetch %d archive snapshots.\n'
        'Image URLs found: %d\n'
        'Archive snapshots without image URL found: %d\n'
        'Images downloads successful: %d\n'
        'Images downloads failed: %d\n'
        'Images uploaded to GCS bucket: %d',
        self.num_snapshots_processed,
        self.num_snapshots_fetch_failed,
        self.num_image_urls_found,
        self.num_ids_without_image_url_found,
        self.num_image_download_success,
        self.num_image_download_failure,
        self.num_image_uploade_to_gcs_bucket)


  def retreive_and_store_images(self, archive_ids):
    logging.info('Processing %d archive snapshots in batches of %d',
        len(archive_ids), self.batch_size)
    try:
      for archive_id_batch in chunks(archive_ids, self.batch_size):
        self.process_archive_images(archive_id_batch)
        self.db_connection.commit()
        logging.info('Processed %d of %d archive snapshots.', self.num_snapshots_processed, len(archive_ids))
        self.log_stats()

    except requests.RequestException as e:
      logging.info('Request exception while processing archive ids:\n%s', e)
      raise(e)

    finally:
      self.log_stats()

  def store_image_in_google_bucket(self, image_dhash, image_bytes):
    image_bucket_path = make_image_hash_file_path(image_dhash)
    blob = self.bucket_client.blob(image_bucket_path)
    blob.upload_from_string(image_bytes)
    self.num_image_uploade_to_gcs_bucket += 1
    logging.debug('Image dhash: %s; uploaded to bucket path: %s', image_dhash, image_bucket_path)
    return blob.public_url

  def process_archive_images(self, archive_id_batch):
    archive_id_to_snapshot_url = construct_snapshot_urls(self.access_token,
        archive_id_batch)
    image_url_to_archive_id = {}
    archive_ids_without_image_url_found = []
    for archive_id, snapshot_url in archive_id_to_snapshot_url.items():
      try:
        image_url_list = get_image_url_list(archive_id, snapshot_url)
      except requests.RequestException as e:
        logging.info('Request exception while processing archive id:%s\n%s',
            archive_id, e)
        self.num_snapshots_fetch_failed += 1

      self.num_snapshots_processed += 1
      if image_url_list:
       for image_url in image_url_list:
         image_url_to_archive_id[image_url] = archive_id

       logging.debug('Archive ID %s has image_url(s): %s', archive_id, image_url_list)
      else:
       logging.warning('Unable to find image_url(s) for archive_id: %s, snapshot_url: '
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
      except requests.RequestException as e:
        logging.info('Exception %s when requesting image_url: %s', e, image_url)
        self.num_image_download_failure += 1
        # TODO(macpd): handle all error types
        image_url_to_fetch_status[image_url] = ImageUrlFetchStatus.UNKNOWN_ERROR
        continue

      self.num_image_download_success += 1
      image_bytes = image_request.content
      image_dhash = get_image_dhash(image_bytes)
      image_sha256 = hashlib.sha256(image_bytes).hexdigest()
      bucket_url = self.store_image_in_google_bucket(image_dhash, image_bytes)
      ad_image_records.append(
          AdImageRecord(
            archive_id=image_url_to_archive_id[image_url],
            fetch_time=fetch_time,
            downloaded_url=image_url,
            bucket_url=bucket_url,
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
      with db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        if max_archive_ids == -1:
          archive_ids = db_interface.all_archive_ids_without_image_hash()
        else:
          archive_ids = db_interface.n_archive_ids_without_image_hash(max_archive_ids)

    bucket_client = make_gcs_bucket_client(GCS_BUCKET, GCS_CREDENTIALS_FILE)
    image_retriever = FacebookAdImageRetriever(db_connection, bucket_client,
        access_token, batch_size)
    image_retriever.retreive_and_store_images(archive_ids)
  finally:
    db_connection.close()

if __name__ == '__main__':
  if len(sys.argv) < 2:
    exit('Usage: %s <config file>' % sys.argv[0])
  main(sys.argv[1:])
