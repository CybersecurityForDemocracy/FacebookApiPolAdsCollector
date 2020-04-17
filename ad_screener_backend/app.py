from collections import defaultdict
import datetime
import json

from flask import current_app, Flask, request, Response
from flask_cors import CORS
import humanize

import config_utils
import db_functions

app = Flask(__name__)

CORS(app, origins=["http://ccs3usr.engineering.nyu.edu:8080",
                   "http://localhost:8080", "http://localhost:5000"])


def load_config(config_path):
    config = config_utils.get_config(config_path)
    app.config['DATABASE_CONNECTION_PARAMS'] = (
        config_utils.get_database_connection_params_from_config(config))
    app.config['FACEBOOK_ACCESS_TOKEN'] = config_utils.get_facebook_access_token(
        config)
    app.config['COUNTRY_CODE'] = config['SEARCH']['COUNTRY_CODE']


load_config('db.cfg')


@app.route('/')
def index():
    return 'Welcome to the ad screening data server. try <a href="./getmockads"/> for data.'

def humanize_int(i):
    """Format numbers for easier readability. Numbers over 1 million are comma formatted, numbers
    over 1 million will be formatted like "1.2 million"

    Args:
        i: int to format.
    Returns:
        string of formatted number.
    """
    if i < 1000000:
        return humanize.intcomma(i)
    return humanize.intword(i)

def get_ad_cluster_record(ad_cluster_data_row):
    ad_cluster_data = {}
    ad_cluster_data['ad_cluster_id'] = ad_cluster_data_row['ad_cluster_id']
    ad_cluster_data['canonical_archive_id'] = ad_cluster_data_row['canonical_archive_id']
    # Ad start/end dates are used for display only, never used for computation
    ad_cluster_data['start_date'] = ad_cluster_data_row['min_ad_creation_time'].strftime('%Y-%m-%d')
    ad_cluster_data['end_date'] = ad_cluster_data_row['max_ad_creation_time'].strftime('%Y-%m-%d')

    # This is the total spend and impression for the ad across all demos/regions
    # Again, used for display and not computation
    # TODO(macpd): use correct currency symbol instead of assuming USD.
    ad_cluster_data['total_spend'] = '$%s - $%s' % (
        humanize_int(int(ad_cluster_data_row['min_spend_sum'])),
        humanize_int(int(ad_cluster_data_row['max_spend_sum'])))
    ad_cluster_data['total_impressions'] = '%s - %s' % (
        humanize_int(int(ad_cluster_data_row['min_impressions_sum'])),
        humanize_int(int(ad_cluster_data_row['max_impressions_sum'])))
    ad_cluster_data['url'] = (
        'https://storage.googleapis.com/facebook_ad_archive_screenshots/%(canonical_archive_id)s.png'
        % ad_cluster_data_row)
    ad_cluster_data['cluster_size'] = humanize_int(int(ad_cluster_data_row['cluster_size']))
    ad_cluster_data['num_pages'] = humanize_int(int(ad_cluster_data_row['num_pages']))
    return ad_cluster_data

@app.route('/getads')
def get_topic_top_ad():
    db_connection = config_utils.get_database_connection(
        current_app.config['DATABASE_CONNECTION_PARAMS'])
    topic_id = request.args.get('topic', None)
    min_date = request.args.get('startDate', None)
    max_date = request.args.get('endDate', None)
    gender = request.args.get('gender', None)
    age_range = request.args.get('ageRange', None)
    region = request.args.get('region', '%')

    # This date parsing is needed because the FE passes raw UTC formatted dates in Zulu time
    # We can simplify this by not sending the time at all from the FE. Then we strip the time info
    # and just take the date for simplicity.
    if min_date and max_date:
        min_date = datetime.datetime.strptime(
            min_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        max_date = datetime.datetime.strptime(
            max_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()

    if gender:
        if gender.lower() == 'all':
            gender = None
        elif gender.lower() == 'f':
            gender = 'female'
        elif gender.lower() == 'm':
            gender = 'male'
        elif gender.lower() == 'u':
            gender = 'unknown'
    if region.lower() == 'all':
        region = None
    if age_range.lower() == 'all':
        age_range = None

    db_interface = db_functions.DBInterface(db_connection)
    ad_cluster_data = db_interface.topic_top_ad_clusters_by_spend(
        topic_id, min_date=min_date, max_date=max_date, region=region, gender=gender,
        age_group=age_range, limit=20)

    ret = {}
    for row in ad_cluster_data:
        ret[row['ad_cluster_id']] = get_ad_cluster_record(row)

    return Response(json.dumps(list(ret.values())), mimetype='application/json')

def make_archive_id_and_image_map(archive_id):
    return {'archive_id': archive_id, 'url':
            'https://storage.googleapis.com/facebook_ad_archive_screenshots/%s.png' % archive_id}

def cluster_additional_ads(db_interface, ad_cluster_id):
    archive_ids = db_interface.ad_cluster_archive_ids(ad_cluster_id)
    return map(make_archive_id_and_image_map, archive_ids)

def cluster_advertiser_info(db_interface, ad_cluster_id):
    advertiser_info = db_interface.ad_cluster_advertiser_info(ad_cluster_id)
    advertiser_info_dict = defaultdict(set)
    for row in advertiser_info:
        if row['page_type']:
            advertiser_info_dict['advertiser_type'].add(row['page_type'])
        if row['party']:
            advertiser_info_dict['advertiser_party'].add(row['party'])
        if row['fec_id']:
            advertiser_info_dict['advertiser_fec_id'].add(row['fec_id'])
        if row['page_url']:
            advertiser_info_dict['advertiser_webiste'].add(row['page_url'])
        if row['advertiser_score']:
            advertiser_info_dict['advertiser_risk_score'].add(str(row['advertiser_score']))

    return {
        'advertiser_type': ', '.join(advertiser_info_dict['advertiser_type']),
        'advertiser_party': ', '.join(advertiser_info_dict['advertiser_party']),
        'advertiser_fec_id': ', '.join(advertiser_info_dict['advertiser_fec_id']),
        'advertiser_webiste': ', '.join(advertiser_info_dict['advertiser_webiste']),
        'advertiser_risk_score': ', '.join(advertiser_info_dict['advertiser_risk_score'])}


@app.route('/getaddetails/<int:ad_cluster_id>')
def get_ad_cluster_details(ad_cluster_id):
    # TODO(macpd): validate ad_cluster_id existence
    db_connection = config_utils.get_database_connection(
        current_app.config['DATABASE_CONNECTION_PARAMS'])
    db_interface = db_functions.DBInterface(db_connection)

    ad_cluster_data = defaultdict(list)
    ad_cluster_data['ad_cluster_id'] = ad_cluster_id
    region_impression_results = db_interface.ad_cluster_region_impression_results(ad_cluster_id)
    for row in region_impression_results:
        ad_cluster_data['region_impression_results'].append(
            {'region': row['region'],
             'min_spend': str(row['min_spend_sum']),
             'max_spend': str(row['max_spend_sum']),
             'min_impressions': row['min_impressions_sum'],
             'max_impressions': row['max_impressions_sum']})

    demo_impression_results = db_interface.ad_cluster_demo_impression_results(ad_cluster_id)
    for row in demo_impression_results:
        ad_cluster_data['demo_impression_results'].append({
            'age_group': row['age_group'],
            'gender': row['gender'],
            'min_spend': str(row['min_spend_sum']),
            'max_spend': str(row['max_spend_sum']),
            'min_impressions': row['min_impressions_sum'],
            'max_impressions': row['max_impressions_sum']})

    ad_cluster_data['advertiser_info'] = cluster_advertiser_info(db_interface, ad_cluster_id)

    ad_cluster_data['funding_entity'] = list(db_interface.ad_cluster_funder_names(ad_cluster_id))
    canonical_archive_id_and_size = db_interface.ad_cluster_canonical_archive_id_and_size(
        ad_cluster_id)
    ad_cluster_data['canonical_archive_id'] = canonical_archive_id_and_size['canonical_archive_id']
    ad_cluster_data['cluster_size'] = canonical_archive_id_and_size['cluster_size']
    ad_cluster_data['url'] = (
        'https://storage.googleapis.com/facebook_ad_archive_screenshots/%s.png' %
        canonical_archive_id)
    ad_cluster_data['alternative_ads'] = list(cluster_additional_ads(db_interface, ad_cluster_id))
    # These fields are generated by NYU and show up in the Metadata tab
    ad_cluster_data['type'] = ', '.join(db_interface.ad_cluster_types(ad_cluster_id))
    ad_cluster_data['entities'] = ', '.join(db_interface.ad_cluster_recognized_entities(
        ad_cluster_id))
    return ad_cluster_data
