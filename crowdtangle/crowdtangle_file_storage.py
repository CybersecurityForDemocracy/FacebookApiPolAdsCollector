import requests
import logging
from fb_ad_creative_retriever import make_gcs_bucket_client,make_image_hash_file_path,upload_blob,get_image_dhash

GCS_CREDENTIALS_FILE = 'gcs_credentials.json'
CROWDTANGLE_BUCKET = 'crowdtangle-media'

def add_crowdtangle_media_to_cloud_storage(media_record):
    crowdtangle_bucket_client = make_gcs_bucket_client(CROWDTANGLE_BUCKET,GCS_CREDENTIALS_FILE)

    image_bytes = requests.get(media_record['url']).content

    image_dhash = get_image_dhash(image_bytes)
    image_bucket_path = make_image_hash_file_path(image_dhash)
    blob_id = upload_blob(crowdtangle_bucket_client, image_bucket_path, image_bytes)

    logging.debug('Image dhash: %s; uploaded to: %s', image_dhash, blob_id)
    media_record['url'] = image_bucket_path
    return media_record
