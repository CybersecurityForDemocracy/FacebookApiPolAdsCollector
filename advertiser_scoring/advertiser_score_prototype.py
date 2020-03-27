import datetime
import numpy as np
import psycopg2
import psycopg2.extras

HOST = 'localhost'
DBNAME = 'fb_global_ads'
USER = 'nyufbpolads'
PASSWORD = 'Y2o$6p3%cLNp'
PORT = 5433

#ads db
DBAuthorize = "host=%s dbname=%s user=%s password=%s port=%s" % (HOST, DBNAME, USER, PASSWORD, PORT)
connection = psycopg2.connect(DBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#advertiser age
page_age = {}
advertiser_age_query = "select page_id, min(ad_creation_time) as creation_time from ads \
where page_id in (select distinct(page_id) from ads where ad_creation_time > '20200101') \
group by page_id"
cursor.execute(advertiser_age_query)
for row in cursor:
    first_date = row['creation_time']
    delta = datetime.datetime.today().date() - first_date
    page_age[row['page_id']] = delta.days

page_age_score = {}
tier_1_cutoff = 365
tier_2_cutoff = 240
tier_3_cutoff = 120
tier_4_cutoff = 90
for page, age in page_age.items():
    score = 0
    if age > tier_1_cutoff:
        score = 1.0
    elif age > tier_2_cutoff:
        score = .75
    elif age > tier_3_cutoff:
        score = .5
    elif age > tier_4_cutoff:
        score = .25
    page_age_score[page] = score

#advertiser size
page_size = {}
advertiser_size_query = "select page_id, sum(min_impressions) as min_impressions from ads \
join impressions on ads.archive_id = impressions.archive_id \
where page_id in (select distinct(page_id) from ads where ad_creation_time > '20200101') \
group by page_id"
cursor.execute(advertiser_size_query)
for row in cursor:
    page_size[row['page_id']] = row['min_impressions']

page_size_score = {}
tier_1_cutoff = np.quantile(list(page_size.values()), 0.7)
tier_2_cutoff = np.quantile(list(page_size.values()), 0.5)
tier_3_cutoff = np.quantile(list(page_size.values()), 0.35)
tier_4_cutoff = np.quantile(list(page_size.values()), 0.15)
for page, size in page_size.items():
    score = 0
    if size > tier_1_cutoff:
        score = 1.0
    elif size > tier_2_cutoff:
        score = .75
    elif size > tier_3_cutoff:
        score = .5
    elif size > tier_4_cutoff:
        score = .25
        
    page_size_score[page] = score

#page quality - % of ads reported for violating community standards
page_fetch_status = {} 
page_quality_query = "select page_id, snapshot_fetch_status, count(*) \
from ad_snapshot_metadata join ads on ad_snapshot_metadata.archive_id = ads.archive_id where \
ads.archive_id in (select archive_id from ads where ad_creation_time > '20200101' and \
archive_id in (select archive_id from ad_countries where country_code = 'US')) \
group by page_id, snapshot_fetch_status"
cursor.execute(page_quality_query)
for row in cursor:
    page = row['page_id']
    if page in page_fetch_status:
        page_fetch_status[row['page_id']][row['snapshot_fetch_status']] = row['count']
    else:
        page_fetch_status[row['page_id']] = {row['snapshot_fetch_status']: row['count']}

UNKNOWN = 0
SUCCESS = 1
NO_CONTENT_FOUND = 2
INVALID_ID_ERROR = 3
AGE_RESTRICTION_ERROR = 4
NO_AD_CREATIVES_FOUND = 5
page_quality = {}

for page, fetch_status_map in page_fetch_status.items():
    if AGE_RESTRICTION_ERROR in fetch_status_map.keys():
        age_restricted_count = fetch_status_map[AGE_RESTRICTION_ERROR]
        total_count = sum(fetch_status_map.values())
        page_quality[page] = (total_count - age_restricted_count)/total_count
    else:
        page_quality[page] = 1


#advertiser score is 50% their age rank, 50% their size rank, and then adjusted by their quality score
advertiser_score = {}
for page, quality in page_quality.items():
    size_score = page_size_score[page]
    age_score = page_age_score[page]
    advertiser_score[page] = quality * ((.5 * size_score) + (.5 * age_score))
