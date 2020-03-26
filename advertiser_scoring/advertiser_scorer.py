"""Module to score pages based on how long they have been advertising, min number of impressions,
and percentage of ads reported for violating community standards."""
from collections import defaultdict, namedtuple
import datetime
import logging
import sys

import numpy as np

import config_utils
import db_functions
from fb_ad_creative_retriever import SnapshotFetchStatus

_AGE_IN_DAYS_TIER_1_CUTOFF = 365
_AGE_IN_DAYS_TIER_2_CUTOFF = 240
_AGE_IN_DAYS_TIER_3_CUTOFF = 120
_AGE_IN_DAYS_TIER_4_CUTOFF = 90
_MIN_IMPRESSIONS_SUM_TIER_1_QUANTILE = 0.7
_MIN_IMPRESSIONS_SUM_TIER_2_QUANTILE = 0.5
_MIN_IMPRESSIONS_SUM_TIER_3_QUANTILE = 0.35
_MIN_IMPRESSIONS_SUM_TIER_4_QUANTILE = 0.15

AdvertiserScoreRecord = namedtuple('AdvertiserScoreRecord', ['page_id', 'advertiser_score'])

def pages_age_scores(page_age_and_min_impressions_sums):
    """Calculate pages scores based on age page's oldest ad.

    Args:
        page_age_and_min_impressions_sums: list of PageAgeAndMinImpressionSum records.
    Returns:
        dict page_id -> age score as float.
    """
    today = datetime.datetime.today().date()
    page_id_to_age_score = {}
    for page_info in page_age_and_min_impressions_sums:
        delta = today - page_info.oldest_ad_date
        age_in_days = delta.days
        score = 0
        if age_in_days > _AGE_IN_DAYS_TIER_1_CUTOFF:
            score = 1.0
        elif age_in_days > _AGE_IN_DAYS_TIER_2_CUTOFF:
            score = .75
        elif age_in_days > _AGE_IN_DAYS_TIER_3_CUTOFF:
            score = .5
        elif age_in_days > _AGE_IN_DAYS_TIER_4_CUTOFF:
            score = .25
        page_id_to_age_score[page_info.page_id] = score

    return page_id_to_age_score


def page_size_scores(page_id_to_min_impressions_sum):
    """Calculate pages scores based on min number of impressions (as a proxy for page size).

    Args:
        page_id_to_min_impressions_sum: dict page_id -> sum of page's ads min impressions.
    Returns:
        dict page_id -> age score as float.
    """
    impressions_sums = list(page_id_to_min_impressions_sum.values())
    tier_1_cutoff = np.quantile(impressions_sums, _MIN_IMPRESSIONS_SUM_TIER_1_QUANTILE)
    tier_2_cutoff = np.quantile(impressions_sums, _MIN_IMPRESSIONS_SUM_TIER_2_QUANTILE)
    tier_3_cutoff = np.quantile(impressions_sums, _MIN_IMPRESSIONS_SUM_TIER_3_QUANTILE)
    tier_4_cutoff = np.quantile(impressions_sums, _MIN_IMPRESSIONS_SUM_TIER_4_QUANTILE)

    page_size_score = {}
    for page_id, size in page_id_to_min_impressions_sum.items():
        score = 0
        if size > tier_1_cutoff:
            score = 1.0
        elif size > tier_2_cutoff:
            score = .75
        elif size > tier_3_cutoff:
            score = .5
        elif size > tier_4_cutoff:
            score = .25
        page_size_score[page_id] = score

    return page_size_score


def page_snapshot_fetch_status_counts_scores(page_id_to_fetch_counts):
    """Calculate pages scores based on ratio of creatives that have been reported for community
    standards violation (as a proxy for page quality)

    Args:
        page_id_to_fetch_counts: dict page_id -> dict of fetch status -> int count of fetch status
        for page's ad creatives.
    Returns:
        dict page_id -> age score as float.
    """
    page_fetch_status_count_score = {}
    for page, fetch_status_map in page_id_to_fetch_counts.items():
        if SnapshotFetchStatus.AGE_RESTRICTION_ERROR in fetch_status_map:
            age_restricted_count = fetch_status_map[SnapshotFetchStatus.AGE_RESTRICTION_ERROR]
            total_count = sum(fetch_status_map.values())
            page_fetch_status_count_score[page] = (total_count - age_restricted_count) / total_count
        else:
            page_fetch_status_count_score[page] = 1

    return page_fetch_status_count_score

def calculate_advertiser_score(fetch_status_score, size_score, age_score):
    """Score is 50% age score, 50% size score, and then adjusted by quality score."""
    return fetch_status_score * ((.5 * size_score) + (.5 * age_score))


def main(config_path):
    config = config_utils.get_config(config_path)
    db_connection_params = config_utils.get_database_connection_params_from_config(config)
    with config_utils.get_database_connection(db_connection_params) as db_connection:
        db_interface = db_functions.DBInterface(db_connection)
        min_ad_creation_time = datetime.date(year=2020, month=1, day=1)
        page_age_and_min_impressions_sum = db_interface.advertisers_age_and_sum_min_impressions(
            min_ad_creation_time)
        logging.info('Got %d age and min_impressions_sums records.',
                     len(page_age_and_min_impressions_sum))
        age_page_ids = {page.page_id for page in page_age_and_min_impressions_sum}
        page_snapshot_status_fetch_counts = db_interface.page_snapshot_status_fetch_counts(
            min_ad_creation_time)
        fetch_status_page_ids = {page.page_id for page in page_snapshot_status_fetch_counts}
        logging.info('Got %d snapshot_fetch_count info records. %d page_ids',
                     len(page_snapshot_status_fetch_counts), len(fetch_status_page_ids))
        page_ids_difference = age_page_ids.symmetric_difference(fetch_status_page_ids)
        logging.info('age_page_ids.symmetric_difference(fetch_status_page_ids): len(%d)',
                     len(page_ids_difference))
        page_ids_intersection = age_page_ids.intersection(fetch_status_page_ids)
        logging.info('age_page_ids.intersection(fetch_status_page_ids): len(%d)',
                     len(page_ids_intersection))

    page_id_to_age_score = pages_age_scores(page_age_and_min_impressions_sum)

    page_id_to_min_impressions_sum = {page.page_id: page.min_impressions_sum for page in
                                      page_age_and_min_impressions_sum}
    page_id_to_size_score = page_size_scores(page_id_to_min_impressions_sum)

    page_id_to_fetch_counts = defaultdict(lambda: dict())
    for fetch_info in page_snapshot_status_fetch_counts:
        page_id_to_fetch_counts[fetch_info.page_id][fetch_info.snapshot_fetch_status] = (
            fetch_info.count)

    page_id_to_fetch_status_score = page_snapshot_fetch_status_counts_scores(
        page_id_to_fetch_counts)

    advertiser_score = {}
    for page_id in page_ids_intersection:
        advertiser_score[page_id] = calculate_advertiser_score(
            page_id_to_fetch_status_score[page_id],
            page_id_to_size_score[page_id],
            page_id_to_age_score[page_id])

    logging.info('Scored %d pages', len(advertiser_score))
    advertiser_score_records = [
        AdvertiserScoreRecord(page_id=k, advertiser_score=v) for k, v in advertiser_score.items()]
    with config_utils.get_database_connection(db_connection_params) as db_connection:
        db_interface = db_functions.DBInterface(db_connection)
        logging.info('Updating advertiser score for %d page IDs', len(advertiser_score_records))
        db_interface.update_advertiser_scores(advertiser_score_records)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    config_utils.configure_logger("advertiser_scorer.log")
    main(sys.argv[1])
