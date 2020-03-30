from collections import defaultdict
import datetime
from flask import Blueprint, current_app, jsonify, request, Response

import json
import config_utils
import db_functions
import snapshot_url_util


topics = Blueprint('topics', __name__, url_prefix='/topics')

@topics.route('/')
def list_all_topics():
    db_connection = current_app.config['DATABASE_CONNECTION']
    db_interface = db_functions.DBInterface(db_connection)
    return db_interface.all_topics()

@topics.route('/<int:topic_id>')
def get_topic_top_ad(topic_id):
    db_connection = current_app.config['DATABASE_CONNECTION']
    country_code = current_app.config['COUNTRY_CODE']
    min_date = request.args.get('min_date', None)
    max_date = request.args.get('max_date', None)
    if min_date and max_date:
        min_date = datetime.datetime.strptime(min_date, '%Y%m%d')
        max_date = datetime.datetime.strptime(max_date, '%Y%m%d')

    db_interface = db_functions.DBInterface(db_connection)
    topic_top_ads_archive_ids = [
        r['archive_id'] for r in db_interface.topic_top_ads_by_spend(country_code,
                                                                     topic_id,
                                                                     min_date=min_date,
                                                                     max_date=max_date)]

    ret = defaultdict(lambda: defaultdict(list))
    region_impression_results = db_interface.region_impression_results(topic_top_ads_archive_ids)
    for row in region_impression_results:
        ret[row['archive_id']]['region_impression_results'].append({
            'region': row['region'], 'min_spend': str(row['min_spend']),
            'max_spend': str(row['max_spend']), 'min_impressions': row['min_impressions'],
            'max_impressions': row['max_impressions']})

    demo_impression_results = db_interface.demo_impression_results(topic_top_ads_archive_ids)
    for row in demo_impression_results:
        ret[row['archive_id']]['demo_impression_results'].append({
            'age_group': row['age_group'],
            'gender': row['gender'],
            'max_spend': str(row['max_spend']),
            'min_impressions': row['min_impressions'],
            'max_impressions': row['max_impressions']})

    archive_id_to_funding_entity = db_interface.ads_funder_names(topic_top_ads_archive_ids)
    for archive_id, funder_name in archive_id_to_funding_entity.items():
        ret[archive_id]['funding_entity'] = funder_name

    archive_id_to_snapshot_url = snapshot_url_util.construct_archive_id_to_snapshot_url_map(
        current_app.config['FACEBOOK_ACCESS_TOKEN'], topic_top_ads_archive_ids)
    for archive_id, url in archive_id_to_snapshot_url.items():
        ret[archive_id]['snapshot_url'] = url
        ret[archive_id]['archive_id'] = archive_id
    return Response(json.dumps(list(ret.values())),  mimetype='application/json')


