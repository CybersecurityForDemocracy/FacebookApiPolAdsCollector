import datetime
import json
import sys

import config_utils
import db_functions

def get_region_impression_results(cursor, archive_ids):
    query = (
        'SELECT archive_id, region, min_spend, max_spend, min_impressions, max_impressions '
        'FROM region_impression_results WHERE archive_id = ANY (%s)')
    cursor.execute(query, (archive_ids,))

def get_demo_impression_results(cursor, archive_ids):
    query = (
        'SELECT archive_id, age_group, gender, min_spend, max_spend, min_impressions, '
        'max_impressions FROM demo_impression_results WHERE archive_id = ANY (%s)')
    cursor.execute(query, (archive_ids,))

def get_recognized_entities(cursor, archive_ids):
    query = (
        'SELECT archive_id, entity_name, entity_type FROM ad_creatives JOIN '
        'ad_creative_to_recognized_entities USING(ad_creative_id) JOIN recognized_entities '
        'USING(entity_id) WHERE archive_id = ANY (%s)')
    cursor.execute(query, (archive_ids,))

def get_topics(cursor, archive_ids):
    query = (
        'SELECT archive_id, topic_name FROM ad_topics JOIN topics USING(topic_id) '
        'WHERE archive_id = ANY (%s)')
    cursor.execute(query, (archive_ids,))

def main(config_path):
    config = config_utils.get_config(config_path)
    db_connection = config_utils.get_database_connection_from_config(config)
    db_interface = db_functions.DBInterface(db_connection)
    cursor = db_interface.get_cursor()
    start_date = datetime.date.today() - datetime.timedelta(days=30)
    #  start_date = datetime.date.today() - datetime.timedelta(days=1)
    ad_data_query = (
        'SELECT archive_id, ad_creative_body, ad_creation_time, ad_delivery_start_time, '
        'ad_delivery_stop_time, page_id, currency, ad_creative_link_caption, '
        'ad_creative_link_title, ad_creative_link_description, funding_entity '
        'FROM ads WHERE ad_creation_time >= %(start_date)s')
    cursor.execute(ad_data_query, {'start_date': start_date})
    print(cursor.query)
    ads_data = {}
    archive_ids = []
    for row in cursor:
        archive_id = int(row['archive_id'])
        archive_ids.append(archive_id)
        ads_data[archive_id] = {
        'archive_id': row['archive_id'],
        'ad_creative_body': row['ad_creative_body'],
        'ad_creation_time': str(row['ad_creation_time']),
        'ad_delivery_start_time': str(row['ad_delivery_start_time']),
        'ad_delivery_stop_time': str(row['ad_delivery_stop_time']),
        'page_id': row['page_id'],
        'currency': row['currency'],
        'ad_creative_link_caption': row['ad_creative_link_caption'],
        'ad_creative_link_title': row['ad_creative_link_title'],
        'ad_creative_link_description': row['ad_creative_link_description'],
        'funding_entity': row['funding_entity'],
        'region_impression_results': [],
        'demo_impression_results': [],
        'recognized_entities': [],
        'topics': []
    }

    region_impression_results = get_region_impression_results(cursor, archive_ids)
    for row in cursor:
        ads_data[row['archive_id']]['region_impression_results'].append({
            'region': row['region'], 'min_spend': str(row['min_spend']),
            'max_spend': str(row['max_spend']), 'min_impressions': row['min_impressions'],
            'max_impressions': row['max_impressions']})

    demo_impression_results = get_demo_impression_results(cursor, archive_ids)
    for row in cursor:
        ads_data[row['archive_id']]['demo_impression_results'].append({
            'age_group': row['age_group'], 'gender': row['gender'],
            'max_spend': str(row['max_spend']), 'min_impressions': row['min_impressions'],
            'max_impressions': row['max_impressions']})

    recognized_entities = get_recognized_entities(cursor, archive_ids)
    for row in cursor:
        ads_data[row['archive_id']]['recognized_entities'].append({'name': row['entity_name'],
                                                                   'type': row['entity_type']})

    topics = get_topics(cursor, archive_ids)
    for row in cursor:
        ads_data[row['archive_id']]['topics'].append(row['topic_name'])


    print('len(ads_data): ', len(ads_data))
    with open('data_expose.json', 'w') as f:
        json.dump(ads_data, f)
        print('Dumped to ', f.name())

    #  archive_id_to_snapshot_url = snapshot_url_util.construct_archive_id_to_snapshot_url_map(
        #  current_app.config['FACEBOOK_ACCESS_TOKEN'], archive_ids)
    #  for archive_id, url in archive_id_to_snapshot_url.items():
        #  ret[archive_id]['snapshot_url'] = url
    #  return ret


if __name__ == '__main__':
    main(sys.argv[1])
