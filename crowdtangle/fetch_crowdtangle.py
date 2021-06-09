from collections import namedtuple
import logging

import apache_beam as beam
from apache_beam.transforms import PTransform
from minet.crowdtangle import CrowdTangleAPIClient
from minet.crowdtangle.exceptions import CrowdTangleError


FetchCrowdTangleArgs = namedtuple('FetchCrowdTangleArgs', ['api_token',
                                                           'start_date',
                                                           'end_date',
                                                           'list_ids',
                                                           'dashboard_id',
                                                           'max_results_to_fetch'])

class FetchCrowdTangle(PTransform):
    def fetch(self, input_args):
        logging.info('in FetchCrowdTangle.fetch input_args: %s', input_args)
        try:
            start_date = input_args.start_date
        except KeyError as e:
            error_msg = "No start date provided. Unable to fetch crowdtangle results"
            logging.error(error_msg)
            yield beam.pvalue.TaggedOutput('errors', error_msg)
            return

        partition_strategy = 500
        format_val = 'raw'
        sort_by = 'date'
        end_date = input_args.end_date
        list_ids = input_args.list_ids
        max_results_to_fetch = input_args.max_results_to_fetch
        query_info_message = (
            'start_date: {start_date}, end_date: {end_date}, '
            'partition_strategy: {partition_strategy}, sort_by: {sort_by}, format: {format}, '
            'max_results_to_fetch: {max_results_to_fetch}, dashboard_id: {dashboard_id}, list_ids: {list_ids}'
            ).format(start_date=start_date, end_date=end_date,
                     partition_strategy=partition_strategy, sort_by=sort_by, format=format_val,
                     max_results_to_fetch=max_results_to_fetch, dashboard_id=input_args.dashboard_id, list_ids=list_ids)
        logging.info('Querying CrowdTangle. %s', query_info_message)
        num_posts = 0
        try:
            crowdtangle_client = CrowdTangleAPIClient(token=input_args.api_token)
            for post in crowdtangle_client.posts(start_date=start_date, end_date=end_date,
                                                 partition_strategy=partition_strategy,
                                                 sort_by=sort_by, format=format_val,
                                                 limit=max_results_to_fetch, list_ids=list_ids):
                num_posts += 1
                post['dashboard_id'] = input_args.dashboard_id
                yield beam.pvalue.TaggedOutput('api_results', post)

            logging.info('CrowdTangle fetch complete. Got %d api_results. query info: %s',
                         num_posts, query_info_message)

        except CrowdTangleError as e:
            error_msg = 'Unable to complete fetch, CrowdTangleError: {!r}'.format(e)
            logging.error(error_msg)
            yield beam.pvalue.TaggedOutput('errors', error_msg)


    def expand(self, p):
        """Returns tagged output of fetched crowdtangle api_results, and error messages
        (if encountered)
        """
        return (
            p | "Fetch CrowdTangle results" >> beam.ParDo(self.fetch).with_outputs('api_results',
                                                                                     'errors'))
