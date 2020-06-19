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

from db_functions import DBInterface
from slack_notifier import notify_slack
import config_utils

DEFAULT_MINIMUM_EXPECTED_NEW_ADS = 10000
DEFAULT_MINIMUM_EXPECTED_NEW_IMPRESSIONS = 10000

SearchRunnerParams = namedtuple(
        'SearchRunnerParams',
        ['country_code',
         'facebook_access_token',
         'sleep_time',
         'request_limit',
         'max_requests',
         'soft_max_runtime_in_seconds'
         ])


FIELDS_TO_REQUEST = [
    "id"
]

class SearchRunner():

    def __init__(self, connection, db, search_runner_params):
        self.country_code = search_runner_params.country_code
        self.connection = connection
        self.db = db
        self.fb_access_token = search_runner_params.facebook_access_token
        self.sleep_time = search_runner_params.sleep_time
        self.request_limit = search_runner_params.request_limit
        self.max_requests = search_runner_params.max_requests
        self.active_ads = []
        self.total_ads_marked_active = 0
        self.graph_error_counts = defaultdict(int)
        self.stop_time = None
        if search_runner_params.soft_max_runtime_in_seconds:
            start_time = time.monotonic()
            soft_deadline = search_runner_params.soft_max_runtime_in_seconds
            self.stop_time = start_time + soft_deadline
            logging.info(
                'Will cease execution after %d seconds.', soft_deadline)


    def run_search(self, page_id=None, page_name=None):
        #get ads
        graph = facebook.GraphAPI(access_token=self.fb_access_token)
        has_next = True
        next_cursor = ""
        backoff_multiplier = 1
        logging.info(datetime.datetime.now())
        request_count = 0
        while (has_next and request_count < self.max_requests and
               self.allowed_execution_time_remaining()):
            request_count += 1
            total_ad_count = 0
            try:
                results = None
                logging.info(f"making active ads request")
                logging.info(f"making request {request_count}")
                results = graph.get_object(
                    id='ads_archive',
                    ad_reached_countries=self.country_code,
                    ad_type='POLITICAL_AND_ISSUE_ADS',
                    ad_active_status='ALL',
                    impression_condition='HAS_IMPRESSIONS_YESTERDAY',
                    limit=self.request_limit,
                    search_terms=page_name,
                    fields=",".join(FIELDS_TO_REQUEST),
                    after=next_cursor)
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
                self.active_ads.append(result.get('id', None))


            #we finished parsing all ads in the result
            self.write_results()

            if "paging" in results and "next" in results["paging"]:
                next_cursor = results["paging"]["cursors"]["after"]
            else:
                has_next = False


    def allowed_execution_time_remaining(self):
        # No deadline configured.
        if self.stop_time is None:
            return True

        if time.monotonic() >= self.stop_time:
            logging.info('Allowed execution time has elapsed. quiting.')
            return False

        return True


    def write_results(self):
        #write new ads to our database
        num_active_ads = len(self.active_ads)
        logging.info("marking %d ads as active today", num_active_ads)
        self.db.update_ad_last_active_date(self.active_ads)
        self.total_ads_marked_active += num_active_ads
        self.active_ads = []
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


    def num_ads_marked(self):
        return self.total_ads_marked_active



def send_completion_slack_notification(
        slack_url, country_code, completion_status, start_time, end_time,
        num_ads_marked_active, num_impressions_added, min_expected_active_ads,
        graph_error_count_string):
    duration_minutes = (end_time - start_time).seconds / 60
    slack_msg_error_prefix = ''
    if (num_ads_marked_active < min_expected_active_ads):
        error_log_msg = (
            f"Minimum expected records not met! Ads expected: "
            f"{min_expected_active_ads} added: {num_ads_marked_active}, ")
        logging.error(error_log_msg)
        slack_msg_error_prefix = (
            ":rotating_light: :rotating_light: :rotating_light: "
            f" {error_log_msg} "
            ":rotating_light: :rotating_light: :rotating_light: ")

    completion_message = (
        f"{slack_msg_error_prefix}Collection started at{start_time} for "
        f"{country_code} completed in {duration_minutes} minutes. Added "
        f" active {num_ads_marked_active} ads. "
        f"Completion status {completion_status}. {graph_error_count_string}")
    notify_slack(slack_url, completion_message)


def main(config):
    logging.info("starting")

    slack_url = config.get('LOGGING', 'SLACK_URL', fallback='')

    if 'MINIMUM_EXPECTED_ACTIVE_ADS' in config['SEARCH']:
        min_expected_active_ads = int(config['SEARCH']['MINIMUM_EXPECTED_ACTIVE_ADS'])
    else:
        min_expected_active_ads = DEFAULT_MINIMUM_EXPECTED_ACTIVE_ADS
    logging.info('Expecting minimum %d active ads.', min_expected_active_ads)

    search_runner_params = SearchRunnerParams(
        country_code=config['SEARCH']['COUNTRY_CODE'],
        facebook_access_token=config_utils.get_facebook_access_token(config),
        sleep_time=config.getint('SEARCH', 'SLEEP_TIME'),
        request_limit=config.getint('SEARCH', 'LIMIT'),
        max_requests=config.getint('SEARCH', 'MAX_REQUESTS'),
        soft_max_runtime_in_seconds=config.getint('SEARCH', 'SOFT_MAX_RUNIME_IN_SECONDS',
                                                  fallback=None))

    connection = config_utils.get_database_connection_from_config(config)
    logging.info('Established conneciton to %s', connection.dsn)
    db = DBInterface(connection)
    search_runner = SearchRunner(
                    connection,
                    db,
                    search_runner_params)
    start_time = datetime.datetime.now()
    country_code_uppercase = search_runner_params.country_code.upper()
    notify_slack(slack_url,
                 f"Starting active ad collection at {start_time} for "
                 f"{country_code_uppercase}")
    completion_status = 'Failure'
    try:
        search_runner.run_search()
        completion_status = 'Success'
    except Exception as e:
        completion_status = f'Uncaught exception: {e}'
        logging.error(completion_status, exc_info=True)
    finally:
        connection.close()
        end_time = datetime.datetime.now()
        num_ads_marked_active = search_runner.num_ads_marked()
        logging.info(search_runner.get_formatted_graph_error_counts())
        send_completion_slack_notification(
            slack_url, country_code_uppercase, completion_status, start_time,
            end_time, num_ads_marked_active,min_expected_active_ads,
            search_runner.get_formatted_graph_error_counts())

if __name__ == '__main__':
    config = config_utils.get_config(sys.argv[1])
    country_code = config['SEARCH']['COUNTRY_CODE'].lower()

    config_utils.configure_logger(f"{country_code}_active_ads_fb_api_collection.log")
    if len(sys.argv) < 2:
        exit(f"Usage:python3 {sys.argv[0]} active_ads_fb_collector.cfg")
    main(config)