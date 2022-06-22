import requests
import logging
import hashlib
from fb_ad_creative_retriever import make_gcs_bucket_client,make_image_hash_file_path,upload_blob,get_image_dhash

def add_crowdtangle_media_to_cloud_storage(media_record, bucket_client):
    if media_record.type != 'photo':
        logging.debug('Skipping non-photo media')
        return media_record

    url = media_record.url_full or media_record.url
    image_bytes = requests.get(url).content

    media_sha256_hash = hashlib.sha256(image_bytes).hexdigest()

    image_dhash = get_image_dhash(image_bytes)
    bucket_path = make_image_hash_file_path(image_dhash)

    blob_id = upload_blob(bucket_client, bucket_path, image_bytes)

    logging.debug('Image dhash: %s; uploaded to: %s', image_dhash, blob_id)
    return media_record._replace(nyu_sim_hash=image_dhash, nyu_sha256_hash=media_sha256_hash,
                          nyu_bucket_path=blob_id)
