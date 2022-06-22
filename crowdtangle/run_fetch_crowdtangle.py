import argparse
import configparser
import datetime
import logging

from typing import Sequence

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import config_utils
from crowdtangle import fetch_crowdtangle
from crowdtangle import process_crowdtangle_posts
from crowdtangle import write_crowdtangle_results_to_database
from crowdtangle import db_functions

GCS_CREDENTIALS_FILE = 'gcs_credentials.json'
CROWDTANGLE_BUCKET = 'crowdtangle-media'

def get_dashboards_fetch_args(config: configparser.ConfigParser,
                              database_connection_params: config_utils.DatabaseConnectionParams) -> Sequence[fetch_crowdtangle.FetchCrowdTangleArgs]:
    """Gets list of config section names from ['CROWDTANGLE']['DASHBOARD_CONFIG_SECTION_NAMES'],
    parses API_TOKEN, DASHBOARD_NAME, LIST_IDS from those named config sections, and returns
    FetchCrowdTangleArgs for each named config section.
    """
    dashboard_config_section_names = (
        config['CROWDTANGLE']['DASHBOARD_CONFIG_SECTION_NAMES'].split(','))

    with config_utils.get_database_connection(database_connection_params) as db_connection:
        db_interface = db_functions.CrowdTangleDBInterface(db_connection)
        dashboard_name_to_id = db_interface.all_dashboards_name_to_id()
        logging.info('Dashboard Names -> IDs: %s', dashboard_name_to_id)

    if 'DAYS_IN_PAST_TO_SYNC' in config['CROWDTANGLE']:
        start_date = (datetime.date.today() -
                      datetime.timedelta(days=config['CROWDTANGLE'].getint('DAYS_IN_PAST_TO_SYNC'))
                      ).isoformat()
        end_date = None
    else:
        start_date = config['CROWDTANGLE'].get('START_DATE')
        end_date = config['CROWDTANGLE'].get('END_DATE', None)

    fetch_args_list = []
    for config_section_name in dashboard_config_section_names:
        config_section = config[config_section_name]
        api_token = config_section.get('API_TOKEN')
        dashboard_name = config_section.get('DASHBOARD_NAME')
        max_results_to_fetch = config_section.getint('MAX_RESULTS_TO_FETCH', None)
        list_ids = config_section.get('LIST_IDS', None)
        if list_ids:
            list_ids = list_ids.split(',')
        rate_limit = config_section.getint('RATE_LIMIT', None)

        fetch_args_list.append(fetch_crowdtangle.FetchCrowdTangleArgs(
                    api_token=api_token,
                    list_ids=list_ids,
                    start_date=start_date,
                    end_date=end_date,
                    dashboard_id=dashboard_name_to_id[dashboard_name],
                    max_results_to_fetch=max_results_to_fetch,
                    rate_limit=rate_limit))
    return fetch_args_list


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_path',
        dest='config_path',
        required=True,
        help='Configuration file path')
    parser.add_argument(
        '--dry_run',
        dest='dry_run',
        action='store_true',
        default=False,
        required=False,
        help='If true, do not write output to database, and print output instead')
    known_args, pipeline_args = parser.parse_known_args(argv)

    if known_args.dry_run:
        logging.info('DRY RUN, will not write output to database, and will print output to stdout.')

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    config = config_utils.get_config(known_args.config_path)
    database_connection_params = config_utils.get_database_connection_params_from_config(config)
    fetch_args_list = get_dashboards_fetch_args(config, database_connection_params)

    logging.info('About to start crowdtangle fetch pipline with args: %s', fetch_args_list)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        results, errors = (
            pipeline | beam.Create(fetch_args_list)
            | 'Fetch CrowdTangle results' >> fetch_crowdtangle.FetchCrowdTangle()
            )

        processed_results = (
            results
            | 'Transform CrowdTangle for SQL' >> beam.ParDo(
                process_crowdtangle_posts.ProcessCrowdTanglePosts())
            | 'Batch CrowdTangle results transformed for SQL' >>
            beam.transforms.util.BatchElements(min_batch_size=10, max_batch_size=500)
            )

        if known_args.dry_run:
            def print_row(row):
                print(row)
                return row
            processed_results | beam.Map(print_row)

        else:
            (processed_results
             | 'Write processed results to Database' >> beam.ParDo(
                 write_crowdtangle_results_to_database.WriteCrowdTangleResultsToDatabase(
                         database_connection_params=database_connection_params,
                         # TODO(macpd): read bucket name and creds file path from config
                         gcs_bucket_name=CROWDTANGLE_BUCKET,
                         gcs_credentials_file=GCS_CREDENTIALS_FILE)))


if __name__ == '__main__':
    config_utils.configure_logger('run_fetch_crowdtangle.log')
    run()
