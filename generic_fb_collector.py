import configparser
import csv
import datetime
import json
import logging
import os
import random
import sys
from collections import defaultdict, namedtuple
from time import sleep
from urllib.parse import parse_qs, urlparse

import facebook
import psycopg2
import psycopg2.extras
from OpenSSL import SSL

from db_functions import DBInterface
from slack_notifier import notify_slack

if len(sys.argv) < 2:
    exit(f"Usage:python3 {sys.argv[1]} generic_fb_collector.cfg")
config = configparser.ConfigParser()
config.read(sys.argv[1])
country_code = config['SEARCH']['COUNTRY_CODE'].lower()
logging.basicConfig(handlers=[logging.FileHandler(f"{country_code}_fb_api_collection.log"),
                              logging.StreamHandler()],
                    format='[%(levelname)s\t%(asctime)s] %(message)s',
                    level=logging.INFO)
#data structures to hold new ads
AdRecord = namedtuple(
    "AdRecord",
    [
        "archive_id",
        "page_id",
        "page_name",
        "ad_snapshot_url",
        "ad_creative_body",
        "funding_entity",
        "ad_creation_time",
        "ad_delivery_start_time",
        "ad_delivery_stop_time",
        "ad_status",
        "impressions__lower_bound",
        "impressions__upper_bound",
        "spend__lower_bound",
        "spend__upper_bound",
        "currency",
        "ad_creative_link_caption",
        "ad_creative_link_description",
        "ad_creative_link_title",
    ],
)
PageRecord = namedtuple("PageRecord", ["id", "name"])
SnapshotRegionRecord = namedtuple(
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
SnapshotDemoRecord = namedtuple(
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

FIELDS_TO_REQUEST = [
    "ad_creation_time",
    "ad_delivery_start_time",
    "ad_delivery_stop_time",
    "ad_snapshot_url",
    "currency",
    "demographic_distribution",
    "impressions",
    "page_id",
    "page_name",
    "region_distribution",
    "spend",
    "ad_creative_body",
    "funding_entity",
    "ad_creative_link_caption",
    "ad_creative_link_description",
    "ad_creative_link_title",
]

class SearchRunner():

    def __init__(self, crawl_date, connection, db, config):
        self.crawl_date = crawl_date
        self.country_code = config['SEARCH']['COUNTRY_CODE']
        self.connection = connection
        self.db = db
        self.fb_access_token = config['FACEBOOK']['TOKEN']
        self.sleep_time = int(config['SEARCH']['SLEEP_TIME'])
        self.request_limit = int(config['SEARCH']['LIMIT'])
        self.max_requests = int(config['SEARCH']['MAX_REQUESTS'])
        self.new_ads = set()
        self.new_funding_entities = set()
        self.new_pages = set()
        self.new_regions = set()
        self.new_impressions = set()
        self.new_ad_region_impressions = list()
        self.new_ad_demo_impressions = list()
        self.existing_pages = None
        self.existing_funding_entities = None
        self.existing_ads_to_end_time_map = None

    def get_ad_from_result(self, result):
        url_parts = urlparse(result['ad_snapshot_url'])
        archive_id = int(parse_qs(url_parts.query)['id'][0])
        ad_status = 1
        if  'ad_delivery_stop_time' in result:
            ad_status = 0
        curr_ad = AdRecord(
            archive_id,
            result.get('page_id', None),
            result.get('page_name', None),
            result.get('ad_snapshot_url', None),
            result.get('ad_creative_body', None),
            result.get('funding_entity', None),
            result.get('ad_creation_time', None),
            result.get('ad_delivery_start_time', None),
            result.get('ad_delivery_stop_time', self.crawl_date),
            ad_status,
            result.get('impressions', dict()).get('lower_bound', '0'),
            result.get('impressions', dict()).get('upper_bound', '0'),
            result.get('spend', dict()).get('lower_bound', '0'),
            result.get('spend', dict()).get('upper_bound', '0'),
            result.get('currency', None),
            result.get('ad_creative_link_caption', None),
            result.get('ad_creative_link_description', None),
            result.get('ad_creative_link_title', None))
        return curr_ad

    
    def process_funding_entity(self, ad):
        if ad.funding_entity not in self.existing_funding_entities:
            # We use tuples because it makes db updates simpler
            self.new_funding_entities.add((ad.funding_entity,))

    def process_page(self, ad):
            if int(ad.page_id) not in self.existing_pages:
                self.new_pages.add(PageRecord(ad.page_id, ad.page_name))
                self.existing_pages.add(int(ad.page_id))

    def process_ad(self, ad):
        if ad.archive_id not in self.existing_ads_to_end_time_map:
            self.new_ads.add(ad)
            self.existing_ads_to_end_time_map[ad.archive_id] = ad.ad_delivery_stop_time

    def process_impressions(self, ad):
        self.new_impressions.add(ad)

    def process_demo_impressions(self, demographic_distribution, curr_ad):
        for demo_result in demographic_distribution:
            self.new_ad_demo_impressions.append(SnapshotDemoRecord(
                curr_ad.archive_id,
                demo_result['age'],
                demo_result['gender'],
                demo_result['percentage'],
                float(demo_result['percentage']) * int(curr_ad.impressions__lower_bound),
                float(demo_result['percentage']) * int(curr_ad.impressions__upper_bound),
                float(demo_result['percentage']) * int(curr_ad.spend__lower_bound),
                float(demo_result['percentage']) * int(curr_ad.spend__upper_bound)))

    def process_region_impressions(self, region_distribution, curr_ad):
        regions = set()
        for region_result in region_distribution: 
            # If we get the same region more than once for an ad, the second occurance
            # This is a data losing proposition but can't be helped till FB fixes the results
            # They provide on the API
            if region_result['region'] in regions:
                continue
            else:
                regions.add(region_result['region'])
            self.new_ad_region_impressions.append(SnapshotRegionRecord(
            curr_ad.archive_id,
            region_result['region'],
            region_result['percentage'],
            float(region_result['percentage']) * int(curr_ad.impressions__lower_bound),
            float(region_result['percentage']) * int(curr_ad.impressions__upper_bound),
            float(region_result['percentage']) * int(curr_ad.spend__lower_bound),
            float(region_result['percentage']) * int(curr_ad.spend__upper_bound)))


    def run_search(self, page_id=None, page_name=None):
        self.crawl_date = datetime.date.today()

        #cache of ads/pages/regions/demo_groups we've already seen so we don't reinsert them
        self.existing_ads_to_end_time_map = self.db.existing_ads()
        self.existing_pages = self.db.existing_pages()
        self.existing_funding_entities = self.db.existing_funding_entities()

        #get ads
        graph = facebook.GraphAPI(access_token=self.fb_access_token)
        has_next = True
        next_cursor = ""
        backoff = 1
        logging.info(datetime.datetime.now())
        logging.info("page_id = %s", page_id)
        logging.info("page_name = %s", page_name)
        request_count = 0
        # TODO: Remove the request_count limit
        #LAE - this is more of a conceptual thing, but perhaps we should be writing to DB more frequently? In cases where we query by the empty string, we are high stakes succeeding or failing.
        curr_ad = None
        while has_next and request_count < self.max_requests:
            #structures to hold all the new stuff we find
            self.new_ads = set()
            self.new_ad_sponsors = set()
            self.new_pages = set()
            self.new_regions = set()
            self.new_impressions = set()
            self.new_ad_region_impressions = list()
            self.new_ad_demo_impressions = list()
            request_count += 1
            try:
                results = None
                if page_name is not None:
                    logging.info(f"making search term request for {page_name}")
                    logging.info(f"making request {request_count}")
                    results = graph.get_object(
                        id='ads_archive',
                        ad_reached_countries=self.country_code,
                        ad_type='POLITICAL_AND_ISSUE_ADS',
                        ad_active_status='ALL',
                        limit=self.request_limit,
                        search_terms=page_name,
                        fields=",".join(FIELDS_TO_REQUEST),
                        after=next_cursor)
                else:
                    logging.info(f"making page_id request for {page_id}")
                    logging.info(f"making request {request_count}")
                    results = graph.get_object(
                        id='ads_archive',
                        ad_reached_countries=self.country_code,
                        ad_type='POLITICAL_AND_ISSUE_ADS',
                        ad_active_status='ALL',
                        limit=self.request_limit,
                        search_page_ids=page_id,
                        fields=",".join(FIELDS_TO_REQUEST),
                        after=next_cursor)
                backoff = 1
            except facebook.GraphAPIError as e:
                backoff += backoff
                logging.error("Graph Error")
                logging.error(e.code)
                logging.error(e)
                if results:
                    logging.error(results)
                else:
                    logging.error("No results")
                if e.code == 4: # this means we've gotten to the FB max results per query
                    sleep(240)
                    has_next = False
                    continue
                else:
                    logging.info("resetting graph")
                    graph = facebook.GraphAPI(access_token=self.fb_access_token)
                    continue
            except OSError as e:
                backoff += backoff
                logging.error("OS error: {0}".format(e))
                logging.error(datetime.datetime.now())
                sleep(60)
                logging.info("resetting graph")
                graph = facebook.GraphAPI(access_token=self.fb_access_token)
                continue

            except SSL.SysCallError as e:
                logging.error(e)
                backoff += backoff
                logging.error("resetting graph")
                graph = facebook.GraphAPI(access_token=self.fb_access_token)
                continue
            finally:
                logging.info(f"waiting for {self.sleep_time} seconds before next query.")
                sleep(self.sleep_time)
            with open(f"response-{request_count}","w") as result_file:
                result_file.write(json.dumps(results))
            old_ad_count = 0
            total_ad_count = 0
            for result in results['data']:
                total_ad_count += 1
                curr_ad = self.get_ad_from_result(result)
                self.process_ad(curr_ad)
                self.process_funding_entity(curr_ad)
                self.process_page(curr_ad)
                self.process_impressions(curr_ad)

                if not curr_ad.ad_status and curr_ad.archive_id in self.existing_ads_to_end_time_map:
                    old_ad_count += 1

                # If this ad is new to the dataset or has updated information, update impressions 
                if (curr_ad.archive_id not in self.existing_ads_to_end_time_map or 
                        # TODO: Test if this date comparison needs typecasting
                        curr_ad.ad_delivery_stop_time != self.existing_ads_to_end_time_map[curr_ad.archive_id]):
                    self.process_impressions(curr_ad)
                    self.process_demo_impressions(result.get('demographic_distribution', []), curr_ad)
                    self.process_region_impressions(result.get('region_distribution', []), curr_ad)

                    if 'demographic_distribution' not in result:
                        logging.info("no demo information in:")
                        logging.info(result)

                    if 'region_distribution' not in result:
                        logging.info("no region information in:")
                        logging.info(result)

            #we finished parsing all ads in the result
            logging.info(f"old ads={old_ad_count}")
            logging.info(f"total ads={total_ad_count}")
            self.write_results()
            self.refresh_state()

            if "paging" in results and "next" in results["paging"]:
                next_cursor = results["paging"]["cursors"]["after"]
            else:
                has_next = False

    def write_results(self):
        #write new pages, regions, and demo groups to self.db first so we can update our caches before writing ads
        self.db.insert_funding_entities(self.new_funding_entities)
        self.db.insert_pages(self.new_pages)
        #write new ads to our database
        logging.info("writing " + str(len(self.new_ads)) + " new ads to db")
        self.db.insert_new_ads(self.new_ads)
        logging.info("writing " + str(len(self.new_impressions)) + " impressions to db")
        self.db.insert_new_impressions(self.new_impressions)

        logging.info("writing self.new_ad_demo_impressions to db")
        self.db.insert_new_impression_demos(self.new_ad_demo_impressions)

        logging.info("writing self.new_ad_region_impressions to db")
        self.db.insert_new_impression_regions(self.new_ad_region_impressions)
        self.connection.commit()

    def refresh_state(self):
        # We have to reload these since we rely on the row ids from the database for indexing
        self.existing_funding_entities = self.db.existing_funding_entities()
        self.connection.commit()



#get page data
def get_page_data(connection, config):
    page_ids = {}
    input_TYPE = config['INPUT']['TYPE']
    if input_TYPE == 'file':
        input_FILES = config['INPUT']['FILES']
        logging.info(input_FILES)
        file_list = json.loads(input_FILES)
        for file_name in file_list:
            with open(file_name) as input:
                for row in input:
                    page_ids[row.strip()] = 0
    else:
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        page_ads_query = "select page_id, count(*) as ad_count from ads group by page_id"
        cursor.execute(page_ads_query)
        for row in cursor:
            page_ids[row['page_id']] = row['ad_count']

    return page_ids


def get_db_connection(config):
    host = config['POSTGRES']['HOST']
    dbname = config['POSTGRES']['DBNAME']
    user = config['POSTGRES']['USER']
    password = config['POSTGRES']['PASSWORD']
    port = config['POSTGRES']['PORT']
    dbauthorize = "host=%s dbname=%s user=%s password=%s port=%s" % (
        host, dbname, user, password, port)
    return psycopg2.connect(dbauthorize)

def get_pages_from_archive(archive_path):
    page_ads = {}
    if not archive_path:
        return page_ads
    with open(archive_path) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row["\ufeffPage ID"] in page_ads:
                page_ads[row["\ufeffPage ID"]] += row["Number of Ads in Library"]
            else:
                page_ads[row["\ufeffPage ID"]] = row["Number of Ads in Library"]

    return page_ads

def main():
    logging.info("starting")
    slack_url = config['LOGGING']['SLACK_URL']
    connection = get_db_connection(config)
    db = DBInterface(connection)
    search_runner = SearchRunner(
        datetime.date.today(),
        connection,
        db,
        config)
    page_ids = get_pages_from_archive(config['INPUT']['ARCHIVE_ADVERTISERS_FILE'])
    page_string = page_ids or 'all pages'
    start_time = datetime.datetime.now()
    notify_slack(slack_url, f"Starting UNIFIED collection at {start_time} for {config['SEARCH']['COUNTRY_CODE']} for {page_string}")
    completion_status = 'Failure'
    try:
        if page_ids:
            curr_page_ids = get_page_data(connection, config)
            for page_id, ad_count in page_ids.items():
                if page_id in curr_page_ids:
                    curr_ad_count = curr_page_ids[page_id]
                    if ad_count > curr_ad_count:
                        page_delta[page_id] = ad_count - curr_ad_count
                else:
                    page_delta[page_id] = ad_count

            prioritized_page_ids = [x for x in sorted(page_delta, key=d.get, reverse=True)]
            #LAE - alter this to work for up to 10 page ids at a time
            for page_id in prioritized_page_ids:
                search_runner.run_search(page_id=page_id)
        else:
            search_runner.run_search(page_name="''")
        completion_status = 'Success'
    except Exception as e:
        completion_status = f'Unchaught exception: {e}'
        logging.error(completion_status, exc_info=True)
    finally:
        end_time = datetime.datetime.now()
        duration_minutes = (end_time - start_time).seconds / 60
        notify_slack(slack_url, f"Collection started at {start_time} for {config['SEARCH']['COUNTRY_CODE']} completed in {duration_minutes} minutes with completion status {completion_status}.")
        connection.close()

if __name__ == '__main__':
    main()
