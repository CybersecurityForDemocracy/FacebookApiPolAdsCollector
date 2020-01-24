#  from bs4 import BeautifulSoup
import configparser
import datetime
import dhash
from google.cloud import storage
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

GCS_BUCKET = 'facebook_ad_images'
GCS_CREDENTIALS_FILE = 'gcs_credentials.json'
logging.basicConfig(handlers=[logging.FileHandler("fb_ad_image_retriever.log"),
                              logging.StreamHandler()],
                    format='[%(levelname)s\t%(asctime)s] {%(pathname)s:%(lineno)d} %(message)s',
                    # TODO(macpd): set this back to INFO
                    level=logging.DEBUG)

#TODO(macpd): don't hardcode this. get this from DB
#  ARCHIVE_IDS= [528030057812924, 544096909523734]
ARCHIVE_IDS= [528030057812924]
IMAGE_URL_JSON_NAME = 'original_image_url'
VIDEO_IMAGE_URL_JSON_NAME = 'video_preview_image_url'
URL_REGEX_TEMPLATE = '"%s":\s*?"(http[^"]+?)"'
IMAGE_URL_REGEX = re.compile(URL_REGEX_TEMPLATE % IMAGE_URL_JSON_NAME)
VIDEO_PREVIEW_IMAGE_URL_REGEX = re.compile(URL_REGEX_TEMPLATE % VIDEO_IMAGE_URL_JSON_NAME)
FB_AD_SNAPSHOT_BASE_URL = 'https://www.facebook.com/ads/archive/render_ad/'

def get_database_connection(config):
  host = config['POSTGRES']['HOST']
  dbname = config['POSTGRES']['DBNAME']
  user = config['POSTGRES']['USER']
  password = config['POSTGRES']['PASSWORD']
  port = config['POSTGRES']['PORT']

  db_authorize = "host=%s dbname=%s user=%s password=%s port=%s" % (host, dbname, user, password, port)
  logging.info('Establishing connecton to %s:%s DB %s', host, port, dbname)
  connection = psycopg2.connect(db_authorize)
  return connection

def get_archive_ids(cursor, limit=200):
  # TODO(macpd): figure out how we want to systematically get archive IDs for this
  archive_ids_sample_query = cursor.mogrify('SELECT archive_id FROM ads LIMIT %s' % limit)
  cursor.execute(archive_ids_sample_query)
  results = cursor.fetchall()
  return [row['archive_id'] for row in results]

def get_snapshot_urls_from_database(cursor, archive_ids):
  query = 'select archive_id, snapshot_url from ads where archive_id in (%s)' % ','.join([str(i) for i in archive_ids])
  logging.debug('Executing query: %s', query)
  cursor.execute(query)
  results = cursor.fetchall()
  archive_id_to_snapshot_url = {}
  for row in results:
    archive_id_to_snapshot_url[row['archive_id']] = row['snapshot_url']
    logging.debug('Archive ID %s has snapshot URL: %s', row['archive_id'], row['snapshot_url'])
  return archive_id_to_snapshot_url

def construct_snapshot_urls(access_token, archive_ids):
  archive_id_to_snapshot_url = {}
  for archive_id in archive_ids:
    url = '%s?%s' % (FB_AD_SNAPSHOT_BASE_URL, urllib.parse.urlencode({'id': archive_id, 'access_token': access_token}))
    logging.debug('Constructed snapshot URL %s', url)
    archive_id_to_snapshot_url[archive_id] = url
  return archive_id_to_snapshot_url

def get_image_url(snapshot_url):
  ad_snapshot_request = requests.get(snapshot_url)
  # TODO(macpd): handle this more gracefully
  # TODO(macpd): check encoding
  ad_snapshot_request.raise_for_status()
  #  soup = BeautifulSoup(ad_snapshot_request.text)
  #  script_tags = soup.find_all(
  ad_snapshot_text = ad_snapshot_request.text
  search_regex = None
  if IMAGE_URL_JSON_NAME in ad_snapshot_text:
    search_regex = IMAGE_URL_REGEX
  elif VIDEO_IMAGE_URL_JSON_NAME in ad_snapshot_text:
    search_regex = VIDEO_PREVIEW_IMAGE_URL_REGEX
  else:
    logging.warn('Expected JSON element not found in ad snapshot: ("%s" OR "%s")', IMAGE_URL_JSON_NAME, VIDEO_PREVIEW_IMAGE_URL_REGEX)
    # TODO(macpd): raise appropriate error here.
    return None

  match = re.search(search_regex, ad_snapshot_text)
  if not match:
    logging.warn('Unable to locate image url in ad snapshot using regex: "%s"', search_regex)
    # TODO(macpd): raise appropriate error here.
    return None
  raw_image_url_str = match.group(1)
  logging.debug('Found raw image URL value in ad snapshot: "%s"', raw_image_url_str)
  image_url = raw_image_url_str.replace('\\', '')
  logging.info('Found image URL value in ad snapshot: "%s"', image_url)
  return image_url

def make_image_hash_file_path(image_hash):
  base_file_name = '%s.jpg' % image_hash
  return os.path.join(image_hash[:4], image_hash[4:8], image_hash[8:12],
                      image_hash[12:16], image_hash[16:20], image_hash[20:24],
                      image_hash[24:28], base_file_name)


def get_image(image_url):
  image_request = requests.get(image_url)
  # TODO(macpd): handle this more gracefully
  # TODO(macpd): check encoding
  image_request.raise_for_status()
  return image_request.content

def get_image_dhash(image_bytes):
  image_file = io.BytesIO(image_bytes)
  image = Image.open(image_file)
  dhash.force_pil()
  row, col = dhash.dhash_row_col(image)
  image_dhash = dhash.format_hex(row, col)
  return image_dhash

def insert_hash_to_database(cursor, archive_id, image_dhash):
  insert_query = cursor.mogrify('INSERT INTO ad_image_hashes(archive_id, ad_image_phash) VALUES (%s, \'%s\')' % (archive_id, image_dhash))
  logging.debug('Constructed archive_id -> phash query: %s', insert_query)
  cursor.execute(insert_query)

def make_gcs_bucket_client(bucket_name, credentials_file):
  storage_client = storage.Client.from_service_account_json(credentials_file)
  bucket_client = storage_client.get_bucket(bucket_name)
  return bucket_client

def store_image_in_google_bucket(bucket_client, image_dhash, image_bytes):
  image_bucket_path = make_image_hash_file_path(image_dhash)
  blob = bucket_client.blob(image_bucket_path)
  blob.upload_from_string(image_bytes)
  logging.info('Image dhash: %s; uploaded to bucket path: %s', image_dhash, image_bucket_path)

def main(argv):
  config = configparser.ConfigParser()
  config.read(argv[0])
  db_connection = get_database_connection(config)
  logging.info('DB connection established')
  cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

  # TODO(macpd): get archive IDs from somewhere, probably the database
  archive_ids = get_archive_ids(cursor, limit=200)
  #  archive_ids = ARCHIVE_IDS
  logging.info('Got %d archive ids: %s', len(archive_ids), ','.join([str(i) for i in archive_ids]))

  access_token = config['FACEBOOK_API']['ACCESS_TOKEN']
  archive_id_to_snapshot_url = construct_snapshot_urls(access_token, archive_ids)
  archive_id_to_image_url = {}
  for archive_id, snapshot_url in archive_id_to_snapshot_url.items():
   image_url = get_image_url(snapshot_url)
   archive_id_to_image_url[archive_id] = image_url
   logging.info('Archive ID %s has image_url: %s', archive_id, image_url)

  bucket_client = make_gcs_bucket_client(GCS_BUCKET, GCS_CREDENTIALS_FILE)
  for archive_id, image_url in  archive_id_to_image_url.items():
    image_bytes = get_image(image_url)
    image_dhash = get_image_dhash(image_bytes)

    store_image_in_google_bucket(bucket_client, image_dhash, image_bytes)
    insert_hash_to_database(cursor, archive_id, image_dhash)

  db_connection.commit()
  db_connection.close()


if __name__ == '__main__':
  if len(sys.argv) < 2:
    exit('Usage: %s <config file>' % sys.argv[0])
  main(sys.argv[1:])
