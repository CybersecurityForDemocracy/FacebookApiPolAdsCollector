from collections import namedtuple
import datetime
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
                                                           'max_results_to_fetch'
                                                           'rate_limit'])

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
        rate_limit = input_args.rate_limit
        query_info_message = (
            'start_date: {start_date}, end_date: {end_date}, '
            'partition_strategy: {partition_strategy}, sort_by: {sort_by}, format: {format}, '
            'max_results_to_fetch: {max_results_to_fetch}, rate_limit: {rate_limit}, '
            'list_ids: {list_ids}, internal dashboard_id: {dashboard_id}'
            ).format(start_date=start_date, end_date=end_date,
                     partition_strategy=partition_strategy, sort_by=sort_by, format=format_val,
                     max_results_to_fetch=max_results_to_fetch, rate_limit=rate_limit,
                     list_ids=list_ids, dashboard_id=input_args.dashboard_id)
        logging.info('Querying CrowdTangle. %s', query_info_message)
        num_posts = 0
        min_seen_updated = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)
        max_seen_updated = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
        try:
            crowdtangle_client = CrowdTangleAPIClient(token=input_args.api_token,
                                                      rate_limit=rate_limit)
            for post in crowdtangle_client.posts(start_date=start_date, end_date=end_date,
                                                 partition_strategy=partition_strategy,
                                                 sort_by=sort_by, format=format_val,
                                                 limit=max_results_to_fetch, list_ids=list_ids):
                num_posts += 1
                post_as_dict = post.as_dict()
                post_as_dict['dashboard_id'] = input_args.dashboard_id
                post_updated = datetime.datetime.fromisoformat(
                    post_as_dict['updated']).replace(tzinfo=datetime.timezone.utc)
                min_seen_updated = min(min_seen_updated, post_updated)
                max_seen_updated = max(max_seen_updated, post_updated)
                yield beam.pvalue.TaggedOutput('api_results', post_as_dict)

            logging.info('CrowdTangle fetch complete. Got %d api_results. query info: %s',
                         num_posts, query_info_message)

        except CrowdTangleError as e:
            error_msg = (
                'Unable to complete fetch, CrowdTangleError: {exc!r} ({exc!s})\n'
                'query info: {query_info}\nprocessed {num_posts} posts. '
                'min post updated: {min_seen_updated}, max post updated: {max_seen_updated}'
            ).format(exc=e, query_info=query_info_message, num_posts=num_posts,
                     min_seen_updated=min_seen_updated, max_seen_updated=max_seen_updated)
            logging.error(error_msg)
            yield beam.pvalue.TaggedOutput('errors', error_msg)


    def expand(self, p):
        """Returns tagged output of fetched crowdtangle api_results, and error messages
        (if encountered)
        """
        return (
            p | "Fetch CrowdTangle results" >> beam.ParDo(self.fetch).with_outputs('api_results',
                                                                                     'errors'))
