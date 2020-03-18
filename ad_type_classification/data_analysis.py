import copy
import psycopg2
import db_functions
import logging
import string
import sys
import urllib
import nltk
import scipy
import pandas

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from urllib import parse
import re 
from ad_type_classification.helper_fns import (get_normalized_creative_url,
                                               get_canonical_url_to_ad_type_table)
from ad_type_classification.text_process_fns import process_creative_body

import config_utils

def distinct_ad_texts(db_connection):
    cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    existing_ad_query = (
        "select  \
        distinct on (ads.ad_creative_body) ads.ad_creative_body, \
        ads.ad_creative_link_title, \
        ads.ad_creative_link_caption, \
        ads.ad_creative_link_description, \
        ad_creative_body, \
        ads.ad_snapshot_url from "
        "ads, ad_countries where \
            ads.ad_creative_body <> '' and \
            ads.archive_id = ad_countries.archive_id and \
            ad_countries.country_code = 'US' \
            order by ads.ad_creative_body, ads.archive_id;")
    cursor.execute(existing_ad_query)
    ads = []
    for row in cursor:
        ads.append(copy.copy(row))
    return ads

config = sys.argv[1]

db_connection = config_utils.get_database_connection_from_config(config)

ads = distinct_ad_texts(db_connection)


ads_df = pandas.DataFrame.from_dict(ads)

# Canonicalized creative bodies
ads_df['processed_body'] = ads_df['ad_creative_body'].apply(process_creative_body)
print(ads_df['processed_body'])

ads_df.to_csv('normed_ads_of_interest.csv')

# get ad classes for known URLS

print(ads_df.columns)
canonical_url_to_ad_type_table = get_canonical_url_to_ad_type_table()
print(canonical_url_to_ad_type_table)
ads_df['normalized_url'] = ads_df.apply(get_normalized_creative_url, axis=1)
ads_df['ad_type'] = ads_df['normalized_url'].apply(
    lambda x : canonical_url_to_ad_type_table.get(x,'UNKNOWN'))

ads_df.to_csv('typed_ads_of_interest.csv')

ads_df1 = ads_df[ads_df['ad_type'] !='UNKNOWN'].sample(frac=0.01)
ads_df1.to_csv('1_percent_ads_of_interest.csv')
