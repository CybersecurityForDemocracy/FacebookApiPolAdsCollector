#%% Init DB
import psycopg2
import db_functions
import logging
import string
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
from helper_fns import get_canonical_url, find_urls, get_database_connection, get_creative_url, get_lookup_table
from text_process_fns import process_creative_body
#%%

db = db_functions.DBInterface(get_database_connection())

# %%
ads = db.distinct_ad_texts()

#%%

ads_df = pandas.DataFrame.from_dict(ads)
# print(ads[0])
# %%

#%% Canonicalized creative bodies
ads_df['processed_body'] = ads_df['ad_creative_body'].apply(process_creative_body)
print(ads_df['processed_body'])

#%%
ads_df.to_csv('normed_ads_of_interest.csv')
# ads_df = pandas.read_csv('/home/divam/projects/ad_types_analysis/normed_ads_of_interest.csv')
# print(ads_df)

#%% get ad classes forknown URLS

# %%
print(ads_df.columns)
lookup_table = get_lookup_table()
print(lookup_table)
ads_df['normalized_url'] = ads_df.apply(get_creative_url, axis=1) 
ads_df['ad_type'] = ads_df['normalized_url'].apply(
    lambda x : lookup_table.get(x,'UNKNOWN'))

# %%

ads_df.to_csv('typed_ads_of_interest.csv')

ads_df1 = ads_df[ads_df['ad_type'] !='UNKNOWN'].sample(frac=0.01)
ads_df1.to_csv('1_percent_ads_of_interest.csv')