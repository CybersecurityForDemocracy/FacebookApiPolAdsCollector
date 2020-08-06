import csv
import datetime
import json
import logging
import operator
import sys
import time
from collections import defaultdict, namedtuple
from time import sleep
from urllib.parse import parse_qs, urlparse

import facebook
import psycopg2
import psycopg2.extras
from OpenSSL import SSL

import db_functions
from slack_notifier import notify_slack
import config_utils

DEFAULT_MINIMUM_EXPECTED_NEW_ADS = 10000
DEFAULT_MINIMUM_EXPECTED_NEW_IMPRESSIONS = 10000

#data structures to hold new ads
AdRecord = namedtuple(
    "AdRecord",
    [
        "ad_creation_time",
        "ad_creative_body",
        "ad_creative_link_caption",
        "ad_creative_link_description",
        "ad_creative_link_title",
        "ad_delivery_start_time",
        "ad_delivery_stop_time",
        "ad_snapshot_url",
        "ad_status",
        "archive_id",
        "country_code",
        "currency",
        "first_crawl_time",
        "funding_entity",
        "impressions__lower_bound",
        "impressions__upper_bound",
        "page_id",
        "page_name",
        "publisher_platform",
        "spend__lower_bound",
        "spend__upper_bound",
        "potential_reach__lower_bound",
        "potential_reach__upper_bound",
    ],
)
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
SearchRunnerParams = namedtuple(
        'SearchRunnerParams',
        ['country_code',
         'facebook_access_token',
         'sleep_time',
         'request_limit',
         'max_requests',
         'stop_at_datetime',
         ])


FIELDS_TO_REQUEST = [
    "ad_creation_time",
    "ad_creative_body",
    "ad_creative_link_caption",
    "ad_creative_link_description",
    "ad_creative_link_title",
    "ad_delivery_start_time",
    "ad_delivery_stop_time",
    "ad_snapshot_url",
    "currency",
    "demographic_distribution",
    "funding_entity",
    "impressions",
    "page_id",
    "page_name",
    "publisher_platform",
    "region_distribution",
    "spend",
    "potential_reach"
]

class SearchRunner():

    def __init__(self, crawl_date, connection, db, search_runner_params):
        self.crawl_date = crawl_date
        self.country_code = search_runner_params.country_code
        self.connection = connection
        self.db = db
        self.fb_access_token = search_runner_params.facebook_access_token
        self.sleep_time = search_runner_params.sleep_time
        self.request_limit = search_runner_params.request_limit
        self.max_requests = search_runner_params.max_requests
        self.new_ads = set()
        self.new_funding_entities = set()
        self.new_pages = set()
        self.deprecated_page_name_records = set()
        self.new_regions = set()
        self.new_impressions = set()
        self.new_ad_region_impressions = list()
        self.new_ad_demo_impressions = list()
        self.existing_page_id_to_page_name = dict()
        self.existing_page_ids = set()
        self.page_id_to_deprecated_page_names_to_deprecated_on_dates = dict()
        self.existing_funding_entities = set()
        self.existing_ads_to_end_time_map = dict()
        self.total_ads_added_to_db = 0
        self.total_impressions_added_to_db = 0
        self.graph_error_counts = defaultdict(int)
        self.stop_time = None
        if search_runner_params.stop_at_datetime:
            self.stop_time = search_runner_params.stop_at_datetime.timestamp()
            logging.info('Will cease execution at %s (timestamp: %s)',
                         search_runner_params.stop_at_datetime, self.stop_time)

    def num_ads_added_to_db(self):
        return self.total_ads_added_to_db

    def num_impressions_added_to_db(self):
        return self.total_impressions_added_to_db

    def get_ad_from_result(self, result):
        url_parts = urlparse(result['ad_snapshot_url'])
        archive_id = int(parse_qs(url_parts.query)['id'][0])
        ad_status = 1
        if  'ad_delivery_stop_time' in result:
            ad_status = 0
        curr_ad = AdRecord(
            ad_creation_time=result.get('ad_creation_time', None),
            ad_creative_body=result.get('ad_creative_body', None),
            ad_creative_link_caption=result.get('ad_creative_link_caption', None),
            ad_creative_link_description=result.get('ad_creative_link_description', None),
            ad_creative_link_title=result.get('ad_creative_link_title', None),
            ad_delivery_start_time=result.get('ad_delivery_start_time', None),
            ad_delivery_stop_time=result.get('ad_delivery_stop_time', None),
            ad_snapshot_url=result.get('ad_snapshot_url', None),
            ad_status=ad_status,
            archive_id=archive_id,
            country_code=self.country_code,
            currency=result.get('currency', None),
            first_crawl_time=self.crawl_date,
            funding_entity=result.get('funding_entity', None),
            impressions__lower_bound=result.get('impressions', dict()).get('lower_bound', '0'),
            impressions__upper_bound=result.get('impressions', dict()).get('upper_bound', '0'),
            page_id=result.get('page_id', None),
            page_name=result.get('page_name', '<NOT PROVIDED>'),
            publisher_platform=result.get('publisher_platform', 'NotProvided'),
            spend__lower_bound=result.get('spend', dict()).get('lower_bound', '0'),
            spend__upper_bound=result.get('spend', dict()).get('upper_bound', '0'),
            potential_reach__lower_bound=result.get(
                'potential_reach', dict()).get('lower_bound', None),
            potential_reach__upper_bound=result.get(
                'potential_reach', dict()).get('upper_bound', None)
        )
        return curr_ad

    def process_funding_entity(self, ad):
        if ad.funding_entity not in self.existing_funding_entities:
            # We use tuples because it makes db updates simpler
            self.new_funding_entities.add((ad.funding_entity,))

    def process_page(self, ad):
        page_id = int(ad.page_id)
        page_record = db_functions.PageRecord(id=page_id, name=ad.page_name)
        if page_id not in self.existing_page_ids:
            self.new_pages.add(page_record)
            self.existing_page_ids.add(page_id)
        # If page_id is known and not a new page for this cycle, but page_name is different. Store
        # it to update the page name. Ignore "bad" page_id 0.
        elif (page_id != 0 and page_record not in self.new_pages and
              page_id in self.existing_page_id_to_page_name and
              self.existing_page_id_to_page_name[page_id] != ad.page_name):
            try:
                ad_creation_time = datetime.datetime.strptime(ad.ad_creation_time,
                                                              '%Y-%m-%dT%H:%M:%S%z')
            except ValueError as err:
                logging.warning('%s unable to parse ad_creation_time %s', err, ad.ad_creation_time)
                return
            page_name_deprecated_on = (
                self.page_id_to_deprecated_page_names_to_deprecated_on_dates.get(
                    page_id, {}).get(ad.page_name, datetime.datetime.min))
            # If ad that has depreacted page name is older than deprecated_on date there's nothing
            # to do.
            if page_name_deprecated_on > ad_creation_time:
                return

            self.deprecated_page_name_records.add(
                db_functions.DeprecatedPageNameRecord(
                    id=page_id, name=self.existing_page_id_to_page_name[page_id],
                    deprecated_on=ad_creation_time))
            # Store new name as a new page so that it is updated.
            self.new_pages.add(page_record)
            logging.info(
                'Page name for page_id %d changned. Old: \'%s\' new: \'%s\' (from ad ID: %s)',
                page_id, self.existing_page_id_to_page_name[page_id], ad.page_name, ad.archive_id)

    def process_ad(self, ad):
        if ad.archive_id not in self.existing_ads_to_end_time_map:
            self.new_ads.add(ad)
            self.existing_ads_to_end_time_map[ad.archive_id] = ad.ad_delivery_stop_time

    def process_impressions(self, ad):
        self.new_impressions.add(ad)

    def process_demo_impressions(self, demographic_distribution, curr_ad):
        if not demographic_distribution:
            logging.info("no demo impression information for: %s", curr_ad.archive_id)

        for demo_result in demographic_distribution:
            try:
                self.new_ad_demo_impressions.append(SnapshotDemoRecord(
                    curr_ad.archive_id,
                    demo_result['age'],
                    demo_result['gender'],
                    demo_result['percentage'],
                    float(demo_result['percentage']) * int(curr_ad.impressions__lower_bound),
                    float(demo_result['percentage']) * int(curr_ad.impressions__upper_bound),
                    float(demo_result['percentage']) * int(curr_ad.spend__lower_bound),
                    float(demo_result['percentage']) * int(curr_ad.spend__upper_bound)))
            except KeyError as key_error:
                logging.warning(
                        '%s error while processing ad archive ID %s demographic_distribution: %s',
                        key_error, curr_ad.archive_id, demo_result)

    def process_region_impressions(self, region_distribution, curr_ad):
        if not region_distribution:
            logging.info("no region impression information for: %s", curr_ad.archive_id)

        regions = set()
        for region_result in region_distribution: 
            # If we get the same region more than once for an ad, the second occurance
            # This is a data losing proposition but can't be helped till FB fixes the results
            # They provide on the API
            try:
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
            except KeyError as key_error:
                logging.warning(
                        '%s error while processing ad archive ID %s region_distribution: %s',
                        key_error, curr_ad.archive_id, region_result)


    def run_search(self, page_id=None, page_name=None):
        self.crawl_date = datetime.date.today()

        #cache of ads/pages/regions/demo_groups we've already seen so we don't reinsert them
        self.existing_ads_to_end_time_map = self.db.existing_ads()
        self.existing_page_id_to_page_name = self.db.existing_pages()
        self.existing_page_ids = set(self.existing_page_id_to_page_name.keys())
        self.page_id_to_deprecated_page_names_to_deprecated_on_dates = self.db.page_id_to_deprecated_page_names()
        self.existing_funding_entities = self.db.existing_funding_entities()

        #get ads
        graph = facebook.GraphAPI(access_token=self.fb_access_token)
        has_next = True
        next_cursor = ""
        backoff_multiplier = 1
        logging.info(datetime.datetime.now())
        logging.info("page_id = %s", page_id)
        logging.info("page_name = %s", page_name)
        request_count = 0
        # TODO: Remove the request_count limit
        #LAE - this is more of a conceptual thing, but perhaps we should be writing to DB more frequently? In cases where we query by the empty string, we are high stakes succeeding or failing.
        curr_ad = None
        while (has_next and request_count < self.max_requests and
               self.allowed_execution_time_remaining()):
            #structures to hold all the new stuff we find
            self.new_ads = set()
            self.new_ad_sponsors = set()
            self.new_funding_entities = set()
            self.new_pages = set()
            self.deprecated_page_name_records = set()
            self.new_regions = set()
            self.new_impressions = set()
            self.new_ad_region_impressions = list()
            self.new_ad_demo_impressions = list()
            request_count += 1
            total_ad_count = 0
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
                backoff_multiplier = 1
            except facebook.GraphAPIError as e:
                logging.error("Graph Error")
                logging.error(e.code)
                logging.error(e)
                self.graph_error_counts[e.code] += 1
                logging.error('Error code %d has occured %d times so far', e.code,
                              self.graph_error_counts[e.code])
                if results:
                    logging.error(results)
                else:
                    logging.error("No results")

                # Error 4 is application level throttling
                # Error 613 is "Custom-level throttling" "Calls to this api have exceeded the rate limit."
                # https://developers.facebook.com/docs/graph-api/using-graph-api/error-handling/
                if e.code == 4 or e.code == 613:
                    backoff_multiplier *= 4
                    logging.info('Rate liimit exceeded, back off multiplier is now %d.',
                                 backoff_multiplier)
                else:
                    backoff_multiplier += 1

                logging.info("resetting graph")
                graph = facebook.GraphAPI(access_token=self.fb_access_token)
                continue

            except OSError as e:
                logging.error("OS error: {0}".format(e))
                logging.error(datetime.datetime.now())
                # Reset backoff multiplier since this is a local OS issue and not an API issue.
                backoff_multiplier = 1
                logging.info("resetting graph")
                graph = facebook.GraphAPI(access_token=self.fb_access_token)
                continue

            except SSL.SysCallError as e:
                logging.error(e)
                backoff_multiplier += backoff_multiplier
                logging.error("resetting graph")
                graph = facebook.GraphAPI(access_token=self.fb_access_token)
                continue

            finally:
                sleep_time = self.sleep_time * backoff_multiplier
                logging.info(f"waiting for {sleep_time} seconds before next query.")
                sleep(sleep_time)

            for result in results['data']:
                total_ad_count += 1
                curr_ad = self.get_ad_from_result(result)
                # Log information about API request and response to debug issue where API returns
                # page_id: "0"
                if curr_ad.page_id is None or int(curr_ad.page_id) == 0:
                    logging.error(
                        'Got bad page_id \'%s\' for archive_id %d.\n'
                        'Args: %s\n'
                        'Full api response:\n%s',
                        curr_ad.page_id, curr_ad.archive_id,
                        {'access_token': self.fb_access_token,
                         'id': 'ads_archive',
                         'ad_reached_countries': self.country_code,
                         'ad_type': 'POLITICAL_AND_ISSUE_ADS',
                         'ad_active_status': 'ALL',
                         'limit': self.request_limit,
                         'search_page_ids': page_id,
                         'fields': ",".join(FIELDS_TO_REQUEST),
                         'after': next_cursor}, result)
                self.process_ad(curr_ad)
                self.process_funding_entity(curr_ad)
                self.process_page(curr_ad)
                self.process_impressions(curr_ad)

                # Update impressions
                self.process_impressions(curr_ad)
                self.process_demo_impressions(result.get('demographic_distribution', []), curr_ad)
                self.process_region_impressions(result.get('region_distribution', []), curr_ad)

            #we finished parsing all ads in the result
            self.write_results()
            self.refresh_state()

            if "paging" in results and "next" in results["paging"]:
                next_cursor = results["paging"]["cursors"]["after"]
            else:
                has_next = False


    def allowed_execution_time_remaining(self):
        # No deadline configured.
        if self.stop_time is None:
            return True

        if time.time() >= self.stop_time:
            logging.info('Exceeded deadline (%s). quitting', self.stop_time)
            return False

        return True


    def write_results(self):
        #write new pages, regions, and demo groups to self.db first so we can update our caches before writing ads
        self.db.insert_funding_entities(self.new_funding_entities)
        self.db.insert_pages(self.new_pages, self.deprecated_page_name_records)

        #write new ads to our database
        num_new_ads = len(self.new_ads)
        logging.info("writing %d new ads to db", num_new_ads)
        self.db.insert_new_ads(self.new_ads)
        self.total_ads_added_to_db += num_new_ads

        #write new impressions to our database
        num_new_impressions = len(self.new_impressions)
        logging.info("writing %d impressions to db", num_new_impressions)
        self.db.insert_new_impressions(self.new_impressions)
        self.total_impressions_added_to_db += num_new_impressions

        logging.info("writing self.new_ad_demo_impressions to db")
        self.db.insert_new_impression_demos(self.new_ad_demo_impressions)

        logging.info("writing self.new_ad_region_impressions to db")
        self.db.insert_new_impression_regions(self.new_ad_region_impressions)
        self.connection.commit()

    def refresh_state(self):
        # We have to reload these since we rely on the row ids from the database for indexing
        self.existing_funding_entities = self.db.existing_funding_entities()
        self.connection.commit()

    def get_formatted_graph_error_counts(self, delimiter='\n'):
        """Get GraphAPI error counts (sorted by count descending) string with specified delimiter.

        Args:
            delimiter: str, used to separate 'error: count' tokens.
        Returns:
            str 'error: count' joined by specified delimiter.
        """
        if not self.graph_error_counts:
            return ''

        count_msgs = [
            '%s: %d' % (error, count) for error, count in sorted(self.graph_error_counts.items(),
                                                                 key=operator.itemgetter(1),
                                                                 reverse=True)]
        return 'GraphAPI error counts %s' % delimiter.join(count_msgs)



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

def send_completion_slack_notification(
        slack_url, country_code, completion_status, start_time, end_time,
        num_ads_added, num_impressions_added, min_expected_new_ads,
        min_expected_new_impressions, graph_error_count_string):
    duration_minutes = (end_time - start_time).seconds / 60
    slack_msg_error_prefix = ''
    if (num_ads_added < min_expected_new_ads or
            num_impressions_added < min_expected_new_impressions):
        error_log_msg = (
            f"Minimum expected records not met! Ads expected: "
            f"{min_expected_new_ads} added: {num_ads_added}, "
            f"impressions expected: {min_expected_new_impressions} added: "
            f"{num_impressions_added} ")
        logging.error(error_log_msg)
        slack_msg_error_prefix = (
            ":rotating_light: :rotating_light: :rotating_light: "
            f" {error_log_msg} "
            ":rotating_light: :rotating_light: :rotating_light: ")

    completion_message = (
        f"{slack_msg_error_prefix}Collection started at{start_time} for "
        f"{country_code} completed in {duration_minutes} minutes. Added "
        f"{num_ads_added} ads, and {num_impressions_added} impressions. "
        f"Completion status {completion_status}. {graph_error_count_string}")
    notify_slack(slack_url, completion_message)

def get_stop_at_datetime(stop_at_time_str):
    """Get datetime for today at the clock time in ISO format.

    Args:
        stop_at_time_str: str time to stop in ISO format. only hours, minutes, seconds used (all other
            info ignored).
    Returns:
        datetime.datetime of today at the specified time.
    """
    stop_at_time = datetime.time.fromisoformat(stop_at_time_str)
    today = datetime.date.today()
    return datetime.datetime(year=today.year, month=today.month, day=today.day,
                             hour=stop_at_time.hour, minute=stop_at_time.minute,
                             second=stop_at_time.second)



def main(config):
    logging.info("starting")

    slack_url = config.get('LOGGING', 'SLACK_URL', fallback='')

    if 'MINIMUM_EXPECTED_NEW_ADS' in config['SEARCH']:
        min_expected_new_ads = int(config['SEARCH']['MINIMUM_EXPECTED_NEW_ADS'])
    else:
        min_expected_new_ads = DEFAULT_MINIMUM_EXPECTED_NEW_ADS
    logging.info('Expecting minimum %d new ads.', min_expected_new_ads)

    if 'MINIMUM_EXPECTED_NEW_IMPRESSIONS' in config['SEARCH']:
        min_expected_new_impressions = int(config['SEARCH']['MINIMUM_EXPECTED_NEW_IMPRESSIONS'])
    else:
        min_expected_new_impressions = DEFAULT_MINIMUM_EXPECTED_NEW_IMPRESSIONS
    logging.info('Expecting minimum %d new impressions.', min_expected_new_impressions)

    stop_at_datetime = get_stop_at_datetime(
        config.get('SEARCH', 'STOP_AT_CLOCK_TIME', fallback='23:55'))

    search_runner_params = SearchRunnerParams(
        country_code=config['SEARCH']['COUNTRY_CODE'],
        facebook_access_token=config_utils.get_facebook_access_token(config),
        sleep_time=config.getint('SEARCH', 'SLEEP_TIME'),
        request_limit=config.getint('SEARCH', 'LIMIT'),
        max_requests=config.getint('SEARCH', 'MAX_REQUESTS'),
        stop_at_datetime=stop_at_datetime)

    connection = config_utils.get_database_connection_from_config(config)
    logging.info('Established conneciton to %s', connection.dsn)
    db = db_functions.DBInterface(connection)
    search_runner = SearchRunner(
        datetime.date.today(),
        connection,
        db,
        search_runner_params)
    page_ids = get_pages_from_archive(config['INPUT']['ARCHIVE_ADVERTISERS_FILE'])
    page_string = page_ids or 'all pages'
    start_time = datetime.datetime.now()
    country_code_uppercase = search_runner_params.country_code.upper()
    notify_slack(slack_url,
                 f"Starting UNIFIED collection at {start_time} for "
                 f"{country_code_uppercase} for {page_string}")
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
        completion_status = f'Uncaught exception: {e}'
        logging.error(completion_status, exc_info=True)
    finally:
        connection.close()
        end_time = datetime.datetime.now()
        num_ads_added = search_runner.num_ads_added_to_db()
        num_impressions_added = search_runner.num_impressions_added_to_db()
        logging.info(search_runner.get_formatted_graph_error_counts())
        send_completion_slack_notification(
            slack_url, country_code_uppercase, completion_status, start_time,
            end_time, num_ads_added, num_impressions_added,
            min_expected_new_ads, min_expected_new_impressions,
            search_runner.get_formatted_graph_error_counts())

if __name__ == '__main__':
    config = config_utils.get_config(sys.argv[1])
    country_code = config['SEARCH']['COUNTRY_CODE'].lower()

    config_utils.configure_logger(f"{country_code}_fb_api_collection.log")
    if len(sys.argv) < 2:
        exit(f"Usage:python3 {sys.argv[0]} generic_fb_collector.cfg")
    main(config)
