import requests
import logging
import hashlib
import io
import os.path

import dhash
from PIL import Image
import tenacity

logger = logging.getLogger()

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
                before_sleep=tenacity.before_sleep_log(logger, logging.INFO))
def upload_blob(bucket_client, blob_path, blob_data):
    blob = bucket_client.blob(blob_path)
    if blob.exists():
        logging.debug('Blob path %s already exists, skipping upload', blob_path)
    else:
        blob.upload_from_string(blob_data)
    return blob.id

def add_crowdtangle_media_to_cloud_storage(media_record, bucket_client):
    if media_record.type != 'photo':
        logging.debug('Skipping non-photo media')
        return media_record

    url = media_record.url_full or media_record.url
    try:
        with requests.get(url) as req
        #  image_bytes = requests.get(url).content
        image_bytes = req.content

        media_sha256_hash = hashlib.sha256(image_bytes).hexdigest()

        image_dhash = get_image_dhash(image_bytes)
    except as e:
        logging.info('Exception %s while processing %s.\nrequest headers: %s', e, media_record,
                     req.headers)
        raise
    bucket_path = make_image_hash_file_path(image_dhash)

    blob_id = upload_blob(bucket_client, bucket_path, image_bytes)

    logging.debug('Image dhash: %s; uploaded to: %s', image_dhash, blob_id)
    return media_record._replace(nyu_sim_hash=image_dhash, nyu_sha256_hash=media_sha256_hash,
                          nyu_bucket_path=blob_id)
