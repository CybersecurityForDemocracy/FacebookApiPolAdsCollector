import json
from collections import defaultdict

import pandas
import psycopg2
import psycopg2.extras
from sklearn.externals import joblib

import config_utils
from helper_fns import (find_urls, get_creative_url, get_lookup_table)
from text_process_fns import process_creative_body

def main(config_file_path):
    config = config_utils.get_config(config_file_path)
    database_connection = config_utils.get_database_connection_from_config(config)
    update_ad_types(list_id_and_types())

    database_connection.close()

def get_all_ads():
    cursor = database_connection.cursor("ad_cursor")
    cursor.execute("select archive_id, ad_creative_body, ad_creative_link_caption from ads where ad_creative_link_caption <> '' or  ad_creative_body <> '';")
    for row in cursor:
        # print(row)
        yield {'archive_id': row[0], 'ad_creative_body': row[1], 'ad_creative_link_caption': row[2]}
    cursor.close()


clfr = joblib.load('ad_type_classifier.pk1')
le = joblib.load('ad_type_label_encoder.pk1')
data_prep = joblib.load('data_prep_pipeline.pk1')

def classify_ads(ads, lookup_table, clfr):
    to_classify = []
    for result in get_all_ads():
        normalized_url = get_creative_url(result)
        if normalized_url in lookup_table:
            yield {'archive_id': result['archive_id'],
                   'ad_type': lookup_table.get(normalized_url)}
        elif result['ad_creative_body']:
            to_classify.append(result)
        else:
            yield {'archive_id': result['archive_id'],
                   'ad_type': 'UNKNOWN'}
        if len(to_classify) > 100000:
            classification_df = pandas.DataFrame(to_classify)
            classification_df['processed_body'] = classification_df['ad_creative_body'].apply(process_creative_body)
            prediction_data = data_prep.transform(classification_df['processed_body'].astype('str'))
            # print(prediction_data)
            classification_df['ad_type']=clfr.predict(prediction_data)
            classification_df['ad_type']=le.inverse_transform(classification_df['ad_type'])
            for result in classification_df.to_dict(orient='records'):
                yield result
            to_classify = []
def update_ad_types(ad_type_map):
    cursor = database_connection.cursor()
    insert_funder_query = (
	"INSERT INTO ad_metadata(archive_id, ad_type) VALUES %s ON CONFLICT (archive_id) DO UPDATE "
        "SET ad_type = EXCLUDED.ad_type")
    insert_template = "(%s, %s)"
    psycopg2.extras.execute_values(cursor,
                                   insert_funder_query,
                                   ad_type_map,
                                   template=insert_template,
                                   page_size=250)
    database_connection.commit()

ad_type_map = defaultdict(list)
lookup_table = get_lookup_table()
for r in classify_ads(get_all_ads(), lookup_table, clfr):
    ad_type_map[r['ad_type']].append(r['archive_id'])
for k, v in ad_type_map.items():
    print(k, len(v))
with open('ad_to_type_mappings.json','w') as w:
    json.dump(ad_type_map, w)

def list_id_and_types():
    for ad_type in ad_type_map:
        for archive_id in ad_type_map[ad_type]:
            yield (archive_id, ad_type)


if __name__ == '__main__':
    config_utils.configure_logger("update_ad_types.log")
    if len(sys.argv) < 2:
        exit(f"Usage:python3 {sys.argv[0]} update_ad_types.cfg")
    main(sys.argv[1])
