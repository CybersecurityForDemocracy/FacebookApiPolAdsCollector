from collections import namedtuple
import logging

import apache_beam as beam
from apache_beam.transforms import PTransform
from minet.crowdtangle import CrowdTangleAPIClient
from minet.crowdtangle.exceptions import CrowdTangleError


FetchCrowdTangleArgs = namedtuple('FetchCrowdTangleArgs', ['start_date',
                                                           'end_date',
                                                           'list_ids',
                                                           'dashboard_name',
                                                           'max_results_to_fetch'])


class FetchCrowdTangle(PTransform):
    def __init__(self, *args, api_token=None, crowdtangle_client=None, **kwargs):
        super().__init__(*args, **kwargs)
        if api_token and crowdtangle_client:
            raise ValueError('api_token and crowdtangle_client args are mutually exclusive.')
        self._api_token = api_token
        self._crowdtangle_client = crowdtangle_client

    def get_crowdtangle_client(self):
        """Returns the CrowdTangleAPIClient provided in the constructor, or creates a new client
        from API token stores in GCP secrets manager.

        This is neccessary because CrowdTangleAPIClient hangs when pickled and then depickled (which
        Apache Beam does sometimes for PTransform objects)
        """
        if self._crowdtangle_client:
            return self._crowdtangle_client

        return CrowdTangleAPIClient(token=self._api_token)

    def fetch(self, input_args):
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
            'max_results_to_fetch: {max_results_to_fetch}, list_ids: {list_ids}'
            ).format(start_date=start_date, end_date=end_date,
                     partition_strategy=partition_strategy, sort_by=sort_by, format=format_val,
                     max_results_to_fetch=max_results_to_fetch, list_ids=list_ids)
        logging.info('Querying CrowdTangle. %s', query_info_message)
        num_posts = 0
        try:
            crowdtangle_client = self.get_crowdtangle_client()
            for post in crowdtangle_client.posts(start_date=start_date, end_date=end_date,
                                                 partition_strategy=partition_strategy,
                                                 sort_by=sort_by, format=format_val,
                                                 limit=max_results_to_fetch, list_ids=list_ids):
                num_posts += 1
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
            p | "Fetch CrowdTangle results" >> beam.FlatMap(self.fetch).with_outputs('api_results',
                                                                                     'errors'))
