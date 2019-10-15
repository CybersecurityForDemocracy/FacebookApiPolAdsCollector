import configparser
import datetime
import json
import logging
import os
import random
import sys
from collections import defaultdict, namedtuple
from time import sleep
from urllib.parse import parse_qs, urlparse
import pandas
import psycopg2
import psycopg2.extras
from OpenSSL import SSL
import facebook
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
        "image_url",
        "text",
        "sponsor_label",
        "creation_date",
        "start_date",
        "end_date",
        "is_active",
        "min_impressions",
        "max_impressions",
        "min_spend",
        "max_spend",
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
        "name",
        "min_impressions",
        "max_impressions",
        "min_spend",
        "max_spend",
        "crawl_date",
    ],
)
SnapshotDemoRecord = namedtuple(
    "SnapshotDemoRecord",
    [
        "archive_id",
        "age_range",
        "gender",
        "min_impressions",
        "max_impressions",
        "min_spend",
        "max_spend",
        "crawl_date",
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

    def __init__(self, crawl_date, country_code, connection, db, fb_access_token, sleep_time):
        self.crawl_date = crawl_date
        self.country_code = country_code
        self.connection = connection
        self.db = db
        self.fb_access_token = fb_access_token
        self.sleep_time = sleep_time

    def get_ad_from_result(self, result):
        image_url = result['ad_snapshot_url']
        url_parts = urlparse(image_url)
        archive_id = int(parse_qs(url_parts.query)['id'][0])
        page_id = result.get('page_id', '')
        page_name = result.get('page_name', '')
        start_date = result['ad_delivery_start_time']
        currency = result['currency']
        ad_text = ''
        if 'ad_creative_body' in result:
            ad_text = result['ad_creative_body']
        ad_sponsor_label = ''
        if 'funding_entity' in result:
            ad_sponsor_label = result['funding_entity']

        is_active = True
        end_date = None
        if 'ad_delivery_stop_time' in result:
            end_date = result['ad_delivery_stop_time']
            is_active = False

        min_impressions = 0
        max_impressions = 0
        min_spend = 0
        max_spend = 0
        if 'impressions' in result:
            min_impressions = result['impressions']['lower_bound']
            max_impressions = result['impressions'].get('upper_bound', -1)
        if 'spend' in result:
            min_spend = result['spend']['lower_bound']
            max_spend = result['spend'].get('upper_bound', -1)

        link_caption = ''
        if 'ad_creative_link_caption' in result:
            link_caption = result['ad_creative_link_caption']
        link_description = ''
        if 'ad_creative_link_description' in result:
            link_description = result['ad_creative_link_description']
        link_title = ''
        if 'ad_creative_link_title' in result:
            link_description = result['ad_creative_link_title']

        curr_ad = AdRecord(
            archive_id,
            page_id,
            page_name,
            image_url,
            ad_text,
            ad_sponsor_label,
            start_date,
            start_date,
            end_date,
            is_active,
            min_impressions,
            max_impressions,
            min_spend,
            max_spend,
            currency,
            link_caption,
            link_description,
            link_title)
        return curr_ad

    def run_search(self, page_id=None, page_name=None):
        self.crawl_date = datetime.date.today()

        #structures to hold all the new stuff we find
        new_ads = set()
        new_ad_sponsors = set()
        new_pages = set()
        new_demo_groups = {}
        new_regions = set()
        new_impressions = set()
        new_ad_region_impressions = defaultdict(dict)
        new_ad_demo_impressions = defaultdict(dict)

        #cache of ads/pages/regions/demo_groups we've already seen so we don't reinsert them
        (ad_ids, active_ads) = self.db.existing_ads()
        existing_regions = self.db.existing_region()
        existing_demo_groups = self.db.existing_demos()
        existing_pages = self.db.existing_page()
        existing_ad_sponsors = self.db.existing_sponsors()

        #get ads
        graph = facebook.GraphAPI(access_token=self.fb_access_token)
        has_next = True
        already_seen = False
        next_cursor = ""
        backoff_time = self.sleep_time
        logging.info(datetime.datetime.now())
        logging.info("page_id = %s", page_id)
        logging.info("page_name = %s", page_name)
        request_count = 0
        # TODO: Remove the request_count limit
        #LAE - this is more of a conceptual thing, but perhaps we should be writing to DB more frequently? In cases where we query by the empty string, we are high stakes succeeding or failing.
        curr_ad = None
        while has_next and not already_seen:
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
                        limit=20000/backoff_time,
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
                        limit=20000/backoff_time,
                        search_page_ids=page_id,
                        fields=",".join(FIELDS_TO_REQUEST),
                        after=next_cursor)
                backoff_time = self.sleep_time
            except facebook.GraphAPIError as e:
                backoff_time += backoff_time
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
                backoff_time += backoff_time
                logging.error("OS error: {0}".format(e))
                logging.error(datetime.datetime.now())
                sleep(60)
                logging.info("resetting graph")
                graph = facebook.GraphAPI(access_token=self.fb_access_token)
                continue

            except SSL.SysCallError as e:
                logging.error(e)
                backoff_time += backoff_time
                logging.error("resetting graph")
                graph = facebook.GraphAPI(access_token=self.fb_access_token)
                continue
            finally:
                logging.info(f"waiting for {backoff_time} seconds before next query.")
                #LAE - I'd like to propose we use a static time for sleeping, since we have an official rate limit from FB of 200 queries per hour. What do you think?
                sleep(backoff_time)
            with open(f"response-{request_count}","w") as result_file:
                result_file.write(json.dumps(results))
            old_ad_count = 0
            total_ad_count = 0
            for result in results['data']:
                total_ad_count += 1
                curr_ad = self.get_ad_from_result(result)
                if curr_ad.sponsor_label not in existing_ad_sponsors:
                    new_ad_sponsors.add(curr_ad.sponsor_label)
                if int(curr_ad.page_id) not in existing_pages:
                    new_pages.add(PageRecord(curr_ad.page_id, curr_ad.page_name))

                if not curr_ad.is_active and curr_ad.archive_id in ad_ids:
                    old_ad_count += 1

                if curr_ad.is_active or curr_ad.archive_id in active_ads or curr_ad.archive_id not in ad_ids:
                    new_impressions.add(curr_ad)
                    if 'demographic_distribution' not in result:
                        logging.info("no demo information in:")
                        logging.info(result)
                        continue

                    if 'region_distribution' not in result:
                        logging.info("no region information in:")
                        logging.info(result)
                        continue

                    for demo_result in result['demographic_distribution']:
                        demo_key = demo_result['age'] + demo_result['gender']
                        new_demo_groups[demo_key] = (demo_result['age'], demo_result['gender'])

                        if demo_key not in new_ad_demo_impressions[curr_ad.archive_id]:
                            new_ad_demo_impressions[curr_ad.archive_id][demo_key] = SnapshotDemoRecord(
                                curr_ad.archive_id,
                                demo_result['age'],
                                demo_result['gender'],
                                float(demo_result['percentage']) * int(curr_ad.min_impressions),
                                float(demo_result['percentage']) * int(curr_ad.max_impressions),
                                float(demo_result['percentage']) * int(curr_ad.min_spend),
                                float(demo_result['percentage']) * int(curr_ad.max_spend),
                                self.crawl_date)

                    for region_result in result['region_distribution']:
                        if region_result['region'] not in existing_regions:
                            new_regions.add(region_result['region'])
                        new_ad_region_impressions[curr_ad.archive_id][region_result['region']] = SnapshotRegionRecord(
                            curr_ad.archive_id,
                            region_result['region'],
                            float(region_result['percentage']) * int(curr_ad.min_impressions),
                            float(region_result['percentage']) * int(curr_ad.max_impressions),
                            float(region_result['percentage']) * int(curr_ad.min_spend),
                            float(region_result['percentage']) * int(curr_ad.max_spend),
                            self.crawl_date)

                if curr_ad.archive_id not in ad_ids:
                    new_ads.add(curr_ad)
                    ad_ids.add(curr_ad.archive_id)

            #we finished parsing each result
            logging.info(f"old ads={old_ad_count}")
            logging.info(f"total ads={total_ad_count}")
            if "paging" in results and "next" in results["paging"]:
                next_cursor = results["paging"]["cursors"]["after"]
            else:
                has_next = False

        #write new pages, regions, and demo groups to self.db first so we can update our caches before writing ads
        self.db.insert_ad_sponsors(new_ad_sponsors)
        self.db.insert_pages(new_pages)
        self.db.insert_regions(new_regions)
        self.db.insert_demos(new_demo_groups)

        self.connection.commit()
        existing_regions = self.db.existing_region()
        existing_demo_groups = self.db.existing_demos()
        existing_pages = self.db.existing_page()
        existing_ad_sponsors = self.db.existing_sponsors()

        #write new ads to our database
        logging.info("writing " + str(len(new_ads)) + " new ads to db")
        #LAE - I think it is incorrect to assume that all ads in a given run will have the same currency. I think this is the source of the SEK bug.
        self.db.insert_new_ads(new_ads, self.country_code, curr_ad.currency, existing_ad_sponsors)

        logging.info("writing " + str(len(new_impressions)) + " impressions to db")
        self.db.insert_new_impressions(new_impressions, self.crawl_date)

        logging.info("writing new_ad_demo_impressions to db")
        self.db.insert_new_impression_demos(new_ad_demo_impressions, existing_demo_groups, self.crawl_date)

        logging.info("writing new_ad_region_impressions to db")
        self.db.insert_new_impression_regions(new_ad_region_impressions, existing_regions, self.crawl_date)
        self.connection.commit()


#get page data
def get_page_data(input_connection, config):
    page_ids = set()
    page_names = set()
    input_TYPE = config['INPUT']['TYPE']
    if input_TYPE == 'file':
        input_FILES = config['INPUT']['FILES']
        logging.info(input_FILES)
        file_list = json.loads(input_FILES)
        for file_name in file_list:
            with open(file_name) as input:
                for row in input:
                    page_ids.add(row.strip())
    else:
        input_cursor = input_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        general_table = config['INPUT']['GENERAL_TABLE']
        weekly_table = config['INPUT']['WEEKLYTABLE']
        search_limit = config['SEARCH']['LIMIT']
        for row in input_cursor:
            this_week_pages_query = f'select page_name, {general_table}.fb_id, total_ads from \
                {weekly_table} join {general_table} \
                on {weekly_table}.nyu_id = {general_table}.nyu_id \
                where week in \
                (select max(week) from {weekly_table}) \
                order by total_ads desc \
                limit({search_limit});'
            input_cursor.execute(this_week_pages_query)
            for row in input_cursor:
                if row['fb_id']:
                    page_ids.add(int(row['fb_id']))
                else:
                    page_names.add(row['page_name'])
    return page_ids, page_names


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
    if not archive_path:
        return []
    #LAE - is there a reason we are using pandas instead of csv?
    df = pandas.read_csv(archive_path)
    return df['Page ID'].to_list()

def main():
    logging.info("starting")
    sleep_time = int(config['SEARCH']['SLEEP_TIME'])
    connection = get_db_connection(config)
    db = DBInterface(connection)
    search_runner = SearchRunner(
        datetime.date.today(),
        config['SEARCH']['COUNTRY_CODE'],
        connection,
        db,
        config['FACEBOOK']['TOKEN'],
        sleep_time)
    page_ids = get_pages_from_archive(config['INPUT']['ARCHIVE_ADVERTISERS_FILE'])
    #LAE - implement thing we discussed where we get existing ads by page id, and then get ads for the pages we are missing ads for, until we have everything
    page_string = page_ids or 'all pages'
    start_time = datetime.datetime.now()
    notify_slack(f"Starting fullscale collection at {start_time} for {config['SEARCH']['COUNTRY_CODE']} for {page_string}")
    completion_status = 'Failure'
    try:
        if page_ids:
            #LAE - alter this to work for up to 10 page ids at a time
            for page_id in page_ids:
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
        notify_slack(f"Collection started at {start_time} for {config['SEARCH']['COUNTRY_CODE']} completed in {duration_minutes} minutes with completion status {completion_status}.")
        connection.close()

if __name__ == '__main__':
    main()
