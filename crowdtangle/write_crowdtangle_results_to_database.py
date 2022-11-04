from operator import attrgetter
import itertools
import logging
import apache_beam as beam
import tenacity
import psycopg2

import config_utils
from crowdtangle import db_functions
from crowdtangle_file_storage import add_crowdtangle_media_to_cloud_storage 

logger = logging.getLogger()

def dedupe_records_with_same_id_by_max_updated_field(records, id_attr_name='id'):
    """Return list of records deduped by ID. If multiple records with the same ID are found the
    record with the highest/latest |updated| is returned.
    """
    record_id_to_latest_updated_record = {}
    for record in records:
        record_id = getattr(record, id_attr_name)
        if record_id in record_id_to_latest_updated_record:
            record_id_to_latest_updated_record[record_id] = max(
                record, record_id_to_latest_updated_record[record_id], key=attrgetter('updated'))
        else:
            record_id_to_latest_updated_record[record_id] = record
    return list(record_id_to_latest_updated_record.values())

def get_account_record_list_only_latest_updated_records(pcoll):
    """Returns list of account records deduped by max updated field."""
    return dedupe_records_with_same_id_by_max_updated_field(
        itertools.chain.from_iterable(map(attrgetter('account_list'), pcoll)))

def get_post_record_list_only_latest_updated_records(pcoll):
    """Returns list of post records deduped by max updated field."""
    return dedupe_records_with_same_id_by_max_updated_field(
        itertools.chain(map(attrgetter('post'), pcoll)))

class WriteCrowdTangleResultsToDatabase(beam.DoFn):
    """DoFn that expects iterables of process_crowdtangle_posts.EncapsulatedPost and writes the
    contained data to database (in order FK relationships reqire).
    """
    def __init__(self, database_connection_params, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._database_connection_params = database_connection_params

    @tenacity.retry(stop=tenacity.stop_after_attempt(3),
                    reraise=True,
                    retry=tenacity.retry_if_exception_type(psycopg2.errors.DeadlockDetected),
                    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
                    before_sleep=tenacity.before_sleep_log(logger, logging.INFO))
    def process(self, pcoll):
        database_connection = config_utils.get_database_connection(self._database_connection_params)
        with database_connection:
            db_interface = db_functions.CrowdTangleDBInterface(database_connection)

            db_interface.upsert_accounts(get_account_record_list_only_latest_updated_records(pcoll))
            db_interface.upsert_posts(get_post_record_list_only_latest_updated_records(pcoll))
            db_interface.upsert_statistics(
                dedupe_records_with_same_id_by_max_updated_field(
                    itertools.chain(map(attrgetter('statistics_actual'), pcoll)),
                    id_attr_name='post_id'),
                dedupe_records_with_same_id_by_max_updated_field(
                    itertools.chain(map(attrgetter('statistics_expected'), pcoll)),
                    id_attr_name='post_id'))
            db_interface.upsert_expanded_links(
                dedupe_records_with_same_id_by_max_updated_field(
                    itertools.chain.from_iterable(map(attrgetter('expanded_links'), pcoll)),
                    id_attr_name='post_id'))
            
            media_records = dedupe_records_with_same_id_by_max_updated_field(itertools.chain.from_iterable(map(attrgetter('media_list'), pcoll)),id_attr_name='post_id')
            for key in media_records:
                media_records[key] = add_crowdtangle_media_to_cloud_storage(media_records[key])

            db_interface.upsert_media(media_records)
            db_interface.insert_post_dashboards({item.post.id: item.dashboard_id for item in pcoll})


# from google.cloud import storage
# import urllib.request

# BUCKET_NAME = "[project_name].appspot.com" # change project_name placeholder to your preferences
# BUCKET_FILE_PATH = "path/to/your/images" # change this path

# def upload_image_from_url_to_google_storage(img_url, img_name):
#     """
#     Uploads an image from a URL source to google storage.
#     - img_url: string URL of the image, e.g. https://picsum.photos/200/200
#     - img_name: string name of the image file to be stored
#     """
#     storage_client = storage.Client()
#     bucket = storage_client.get_bucket(BUCKET_NAME)
#     blob = bucket.blob(BUCKET_FILE_PATH + "/" + img_name + ".jpg")

#     # try to read the image URL
#     try:
#         with urllib.request.urlopen(img_url) as response:
#             # check if URL contains an image
#             info = response.info()
#             if(info.get_content_type().startswith("image")):
#                 blob.upload_from_string(response.read(), content_type=info.get_content_type())
#                 print("Uploaded image from: " + img_url)
#             else:
#                 print("Could not upload image. No image data type in URL")
#     except Exception as e:
#         print('Could not upload image. Generic exception: ', e)