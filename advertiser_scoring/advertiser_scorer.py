from collections import defaultdict
import datetime
import json
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

def page_age_score(oldest_ad_date):
    delta = datetime.datetime.today().date() - oldest_ad_date
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
    return score


#advertiser size
def pages_size_score(page_id_to_min_impressions_sum):
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


#page quality - % of ads reported for violating community standards
def page_snapshot_fetch_status_counts_scores(page_id_to_fetch_counts):
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
        return fetch_status_score * ((.5 * size_score) + (.5 * age_score))


def main(config_path):
    config = config_utils.get_config(config_path)
    db_connection_params = config_utils.get_database_connection_params_from_config(config)
    with config_utils.get_database_connection(db_connection_params) as db_connection:
        db_interface = db_functions.DBInterface(db_connection)
        #  min_ad_creation_time = datetime.date.today() - datetime.timedelta(days=30)
        min_ad_creation_time = datetime.date(year=2015, month=1, day=1)
        page_age_and_min_impressions_sum = db_interface.advertisers_age_and_sum_min_impressions(
            min_ad_creation_time)
        logging.info('Got %d age and min_impressions_sums records.',
                     len(page_age_and_min_impressions_sum))
        age_page_ids = {page.page_id for page in page_age_and_min_impressions_sum}
        page_snapshot_status_fetch_counts = db_interface.page_snapshot_status_fetch_counts(
            min_ad_creation_time, country_code='US')
        fetch_status_page_ids = {page.page_id for page in page_snapshot_status_fetch_counts}
        logging.info('Got %d snapshot_fetch_count info records.',
                     len(page_snapshot_status_fetch_counts))
        page_ids_difference = age_page_ids.symmetric_difference(fetch_status_page_ids)
        logging.info('age_page_ids.symmetric_difference(fetch_status_page_ids): %s len(%d)',
                     page_ids_difference, len(page_ids_difference))
        page_ids_intersection = age_page_ids.intersection(fetch_status_page_ids)
        logging.info('age_page_ids.intersection(fetch_status_page_ids): len(%d)',
                     len(page_ids_intersection))

    page_id_to_age_score = {}
    for page_info in page_age_and_min_impressions_sum:
        page_id_to_age_score[page_info.page_id] = page_age_score(page_info.oldest_ad_date)

    page_id_to_min_impressions_sum = {page.page_id: page.min_impressions_sum for page in
                  page_age_and_min_impressions_sum}
    page_id_to_size_score = pages_size_score(page_id_to_min_impressions_sum)

    page_id_to_fetch_counts = defaultdict(lambda: dict())
    for fetch_info in page_snapshot_status_fetch_counts:
        page_id_to_fetch_counts[fetch_info.page_id][fetch_info.snapshot_fetch_status] = (
            fetch_info.count)

    page_id_to_fetch_status_score = page_snapshot_fetch_status_counts_scores(
        page_id_to_fetch_counts)

    #advertiser score is 50% their age rank, 50% their size rank, and then adjusted by their quality score
    advertiser_score = {}
    for page_id in page_ids_intersection:
        fetch_status_score = page_id_to_fetch_status_score[page_id]
        size_score = page_id_to_size_score[page_id]
        age_score = page_id_to_age_score[page_id]
        advertiser_score[page_id] = calculate_advertiser_score(
            fetch_status_score, size_score, age_score)

    logging.info('Scored %d pages', len(advertiser_score))

    advertiser_score_outfile = "advertiser_score.json"
    with open(advertiser_score_outfile, 'w') as f:
        json.dump(advertiser_score, f)
    logging.info('Wrote advertiser score info to %s', advertiser_score_outfile)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit('Usage: %s <config file>' % sys.argv[0])
    config_utils.configure_logger("advertiser_scorer.log")
    main(sys.argv[1])
