import argparse
import datetime
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from crowdtangle import fetch_crowdtangle
from crowdtangle import process_crowdtangle_posts
from crowdtangle import write_crowdtangle_results_to_database

import config_utils
from crowdtangle import db_functions

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config_path',
        dest='config_path',
        required=True,
        help='Configuration file path')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    config = config_utils.get_config(known_args.config_path)
    max_results_to_fetch = config['CROWDTANGLE'].getint('MAX_RESULTS_TO_FETCH', None)
    if 'DAYS_IN_PAST_TO_SYNC' in config['CROWDTANGLE']:
        start_date = (datetime.date.today() -
                      datetime.timedelta(days=config['CROWDTANGLE'].getint('DAYS_IN_PAST_TO_SYNC'))
                      ).isoformat()
        end_date = None
    else:
        start_date = config['CROWDTANGLE'].get('START_DATE')
        end_date = config['CROWDTANGLE'].get('END_DATE', None)
    api_token = config['CROWDTANGLE'].get('API_TOKEN')
    list_ids = config['CROWDTANGLE'].get('LIST_IDS', None)
    dashboard_name = config['CROWDTANGLE'].get('DASHBOARD_NAME')
    if list_ids:
        list_ids = list_ids.split(',')
    database_connection_params = config_utils.get_database_connection_params_from_config(config)
    with config_utils.get_database_connection(database_connection_params) as db_connection:

        db_interface = db_functions.CrowdTangleDBInterface(db_connection)
        dashboard_name_to_id = db_interface.all_dashboards_name_to_id()

    fetch_crowdtangle_args = fetch_crowdtangle.FetchCrowdTangleArgs(
                list_ids=list_ids,
                start_date=start_date,
                end_date=end_date,
                dashboard_id=dashboard_name_to_id[dashboard_name],
                max_results_to_fetch=max_results_to_fetch)

    logging.info('About to start crowdtangle fetch pipline with args: %s', fetch_crowdtangle_args)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        results, errors = (
            pipeline | beam.Create([fetch_crowdtangle_args])
            | 'Fetch CrowdTangle results' >> fetch_crowdtangle.FetchCrowdTangle(api_token=api_token)
            )

        processed_results = (
            results
            | 'Transform CrowdTangle for SQL' >> beam.ParDo(
                process_crowdtangle_posts.ProcessCrowdTanglePosts())
            | 'Batch CrowdTangle results transformed for SQL' >>
            beam.transforms.util.BatchElements(min_batch_size=10, max_batch_size=500)
            )

        (processed_results
         | 'Write processed results to Database' >> beam.ParDo(
             write_crowdtangle_results_to_database.WriteCrowdTangleResultsToDatabase(
                     database_connection_params)))


if __name__ == '__main__':
    config_utils.configure_logger('run_fetch_crowdtangle.log')
    run()
