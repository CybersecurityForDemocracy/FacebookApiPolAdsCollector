import snapshot_url_util
import db_functions
from flask import Blueprint, current_app, jsonify, request, Response
import datetime
from collections import defaultdict
from flask import Flask, request
from flask_cors import CORS
import config_utils

import json

app = Flask(__name__)


CORS(app, origins=["http://ccs3usr.engineering.nyu.edu:8080",
                   "http://localhost:8080", "http://localhost:5000"])


def load_config(config_path):
    config = config_utils.get_config(config_path)
    app.config['DATABASE_CONNECTION_PARAMS'] = (
        config_utils.get_database_connection_params_from_config(config))
    app.config['DATABASE_CONNECTION'] = config_utils.get_database_connection(
        app.config['DATABASE_CONNECTION_PARAMS'])
    app.config['FACEBOOK_ACCESS_TOKEN'] = config_utils.get_facebook_access_token(
        config)
    app.config['COUNTRY_CODE'] = config['SEARCH']['COUNTRY_CODE']


load_config('db.cfg')


@app.route('/')
def index():
    return 'Welcome to the ad screening data server. try <a href="./getmockads"/> for data.'


def get_dummy_ad():
    """Creates a dummy canonical ad"""
    ad = defaultdict(list)
    # This is assumed unique for every ad and used to generate image links and as an index
    canonical_archive_id = '377259572962960'
    ad['canonical_archive_id'] = canonical_archive_id
    # Funding entity is treated as plain text
    ad['funding_entity'] = 'Veridian Dynamics'

    # Ad start/end dates are used for display only, never used for computation
    ad['start_date'] = '2020-12-23'
    ad['end_date'] = '2020-12-30'

    # This is the total spend and impression for the ad across all demos/regions
    # Again, used for display and not computation
    ad['total_spend'] = '0-99 USD'
    ad['total_impressions'] = '1000-5000'

    # This is the spend/impressions per region, shows up in the AdDetails Regions tab
    ad['region_impression_results'] = [
        {'region': 'Demo Region',
         'min_spend': '0',
         'max_spend': '49.5',
         'min_impressions': '500',
         'max_impressions': '2500'},
        {'region': 'Demo Region 2',
         'min_spend': '0',
         'max_spend': '49.5',
         'min_impressions': '500',
         'max_impressions': '2500'}]

    # This is the spend/impressions per demographic group, shows up in the AdDetails Demos tab
    ad['demo_impression_results'] = [{
        'age_group': '18-24',
        'gender': 'female',
        'min_spend': '0',
        'max_spend': '33',
        'min_impressions': '1000',
        'max_impressions': '1333'},
        {
        'age_group': '65+',
        'gender': 'male',
        'min_spend': '0',
        'max_spend': '33',
        'min_impressions': '1000',
        'max_impressions': '1333'},
        {
        'age_group': '35-44',
        'gender': 'unknown',
        'min_spend': '0',
        'max_spend': '33',
        'min_impressions': '1000',
        'max_impressions': '1333'},
    ]

    # These are used to generate image urls for the alternative AdDetails Alternate Creatives tab
    # additional alternative_archive_ids for this ad if you'd like more results. '354236975482127', '565888870688521'
    ad['alternative_ads'] = [{
        'archive_id': '488483418381824', 'url': 'https://storage.googleapis.com/facebook_ad_archive_screenshots/488483418381824.png'},{
        'archive_id': '1658295924303363', 'url': 'https://storage.googleapis.com/facebook_ad_archive_screenshots/1658295924303363.png'}]
    ad['url'] = f'https://storage.googleapis.com/facebook_ad_archive_screenshots/{canonical_archive_id}.png'
    # These fields are generated by NYU and show up in the Metadata tab
    ad['type'] = 'Commercial'
    ad['entities'] = 'Coronavirus, Kentuky, SmartNews'
    ad['advertizer_type'] = 'set_me!'
    ad['advertizer_party'] = 'set_me!'
    ad['advertizer_fec_id'] = 'set_me!'
    ad['advertizer_webiste'] = 'set_me!'
    ad['advertizer_risk_score'] = 'set_me!'
    return ad


@app.route('/getmockads')
def get_mock_ads():
    """ This function returns a functional mock ad for the front end to display.
    It also verifies that the frontend sent the correct paramters to generate a real response.

    A sample of the get request args is:
    ImmutableMultiDict([('startDate', '2020-03-25T21:49:25.303Z'),
                        ('endDate', '2020-04-01T21:49:25.303Z'),
                        ('topic', '914'),
                        ('region', 'All'),
                        ('gender', 'ALL'),
                        ('ageRange', 'ALL')])
    """
    # All data coming from the server is in camelCase, we can convert it into snake_case as we get the fields
    # the FE doesn't get that lucky, everything in snake_case on the FE came from the server
    assert request.args.get('startDate')
    assert request.args.get('endDate')
    # strings mapping to facebook age buckets: '18-24' etc,
    assert request.args.get('ageRange')
    # The following come from the drop downs on the FE and are hardcoded in App.js
    # Range of topic IDs, the mapping is also stored in App.js
    assert request.args.get('topic')
    # Currently US states as full text: 'New York' (not 'NY')
    assert request.args.get('region')
    assert request.args.get('gender')  # 'male','female','unknown' or 'all'
    assert request.args.get('riskScore')

    # Just return some copies of a dummy ad for the UI to be testable
    return Response(json.dumps([get_dummy_ad(), get_dummy_ad(), get_dummy_ad(), get_dummy_ad(), get_dummy_ad()]),  mimetype='application/json')


def get_ad_cluster_record(ad_cluster_data_row):
    ad_cluster_data = {}
    ad_cluster_data['canonical_archive_id'] = ad_cluster_data_row['canonical_archive_id']
    # Ad start/end dates are used for display only, never used for computation
    ad_cluster_data['start_date'] = ad_cluster_data_row['min_ad_creation_time'].strftime('%Y-%m-%d')
    ad_cluster_data['end_date'] = ad_cluster_data_row['max_ad_creation_time'].strftime('%Y-%m-%d')

    # This is the total spend and impression for the ad across all demos/regions
    # Again, used for display and not computation
    ad_cluster_data['total_spend'] = '%(min_spend_sum)s-%(max_spend_sum)s USD' % ad_cluster_data_row
    ad_cluster_data['total_impressions'] = '%(min_impressions_sum)s-%(max_impressions_sum)s' % ad_cluster_data_row
    ad_cluster_data['url'] = (
        'https://storage.googleapis.com/facebook_ad_archive_screenshots/%(canonical_archive_id)s.png'
        % ad_cluster_data_row)
    return ad_cluster_data

@app.route('/getads')
def get_topic_top_ad():
    # This is a prototype impl with real data, it uses archive_ids instead of deduped clusters
    db_connection = current_app.config['DATABASE_CONNECTION']
    topic_id = request.args.get('topic', None)
    min_date = request.args.get('startDate', None)
    max_date = request.args.get('endDate', None)
    gender = request.args.get('gender', None)
    age_range = request.args.get('ageRange', None)
    region = request.args.get('region', '%')

    # This date parsing is needed because the FE passes raw UTC formatted dates in Zulu time
    # We can simplify this by not sending the time at all from the FE
    if min_date and max_date:
        min_date = datetime.datetime.strptime(
            min_date, "%Y-%m-%dT%H:%M:%S.%fZ")
        max_date = datetime.datetime.strptime(
            max_date, "%Y-%m-%dT%H:%M:%S.%fZ")

    db_interface = db_functions.DBInterface(db_connection)
    ad_cluster_data = db_interface.topic_top_ad_clusters_by_spend(topic_id, min_date=min_date,
                                                                  max_date=max_date, limit=6)

    ret = {}
    for row in ad_cluster_data:
        ret[row['ad_cluster_id']] = get_ad_cluster_record(row)

    return Response(json.dumps(list(ret.values())),  mimetype='application/json')

def make_archive_id_and_image_map(archive_id):
    return {'archive_id': archive_id, 'url':
            'https://storage.googleapis.com/facebook_ad_archive_screenshots/%s.png' % archive_id}

def cluster_additional_ads(ad_cluster_id):
    db_connection = current_app.config['DATABASE_CONNECTION']
    db_interface = db_functions.DBInterface(db_connection)
    archive_ids = db_interface.ad_cluster_archive_ids(ad_cluster_id)
    return map(make_archive_id_and_image_map, archive_ids)


@app.route('/getaddetails/<int:ad_cluster_id>')
def get_ad_cluster_details(ad_cluster_id):
    # TODO(macpd): validate ad_cluster_id existence
    db_connection = current_app.config['DATABASE_CONNECTION']
    db_interface = db_functions.DBInterface(db_connection)
    #  ad_cluster_id = request.args.get('ad_cluster_id')

    ad_cluster_data = {}
    region_impression_results = db_interface.ad_cluster_region_impression_results(ad_cluster_id)
    for row in region_impression_results:
        ad_cluster_data['region_impression_results'].append(
            {'region': row['region'],
             'min_spend': str(row['min_spend']),
             'max_spend': str(row['max_spend']),
             'min_impressions': row['min_impressions'],
             'max_impressions': row['max_impressions']})

    demo_impression_results = db_interface.ad_cluster_demo_impression_results(ad_cluster_id)
    for row in demo_impression_results:
        ad_cluster_data['demo_impression_results'].append({
            'age_group': row['age_group'],
            'gender': row['gender'],
            'min_spend': str(row['min_spend']),
            'max_spend': str(row['max_spend']),
            'min_impressions': row['min_impressions'],
            'max_impressions': row['max_impressions']})

    ad_cluster_data['funding_entity'] = list(db_interface.ad_cluster_funder_names(ad_cluster_id))
    #  ad_cluster_data['alternative_ads'] = [{
        #  'archive_id': '488483418381824', 'url': 'https://storage.googleapis.com/facebook_ad_archive_screenshots/488483418381824.png'},{
        #  'archive_id': '1658295924303363', 'url': 'https://storage.googleapis.com/facebook_ad_archive_screenshots/1658295924303363.png'}]
    # These are used to generate image urls for the alternative AdDetails Alternate Creatives tab
    # additional alternative_archive_ids for this ad_cluster_data if you'd like more results. '354236975482127', '565888870688521'
    canonical_archive_id = db_interface.ad_cluster_canonical_archive_id(ad_cluster_id)
    ad_cluster_data['url'] = (
        'https://storage.googleapis.com/facebook_ad_archive_screenshots/%s.png' %
        canonical_archive_id)
    ad_cluster_data['alternative_ads'] = list(cluster_additional_ads(ad_cluster_id))
    # These fields are generated by NYU and show up in the Metadata tab
    ad_cluster_data['type'] = ', '.join(db_interface.ad_cluster_types(ad_cluster_id))
    ad_cluster_data['entities'] = ', '.join(db_interface.ad_cluster_recognized_entities(
        ad_cluster_id))
    #  ad_cluster_data['advertizer_type'] = 'set_me!'
    #  ad_cluster_data['advertizer_party'] = 'set_me!'
    #  ad_cluster_data['advertizer_fec_id'] = 'set_me!'
    #  ad_cluster_data['advertizer_webiste'] = 'set_me!'
    #  ad_cluster_data['advertizer_risk_score'] = 'set_me!'
    return ad_cluster_data

