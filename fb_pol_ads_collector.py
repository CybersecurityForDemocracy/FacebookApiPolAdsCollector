from urllib.parse import urlparse, parse_qs
import configparser
from collections import namedtuple
from time import sleep
import datetime
import os
import sys
import psycopg2
import psycopg2.extras
import facebook

if len(sys.argv) < 2:
    exit("Usage:python3 import_ads_to_db.py import_ads_to_db.cfg")

config = configparser.ConfigParser()
config.read(sys.argv[1])

crawl_date = datetime.date.today() 

#setup our db cursor
HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
PORT = config['POSTGRES']['PORT']
DBAuthorize = "host=%s dbname=%s user=%s password=%s port=%s" % (HOST, DBNAME, USER, PASSWORD, PORT)
connection = psycopg2.connect(DBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#cache ads/pages/regions/demo_groups we've already seen
existing_ad_query = "select id, is_active from ads"
cursor.execute(existing_ad_query)
ad_ids = set()
active_ads = set()
for row in cursor:
    ad_ids.add(row['archive_id'])
    if row['is_active'] == True:
        active_ads.ad(row['archive_id'])

existing_regions = {}
existing_regions_query = "select name, id from regions;"
cursor.execute(existing_regions_query)
for row in cursor:
    existing_regions[row['name']] = row['id']

existing_demo_groups = {}
existing_demo_group_query = "select gender, age, id from demo_groups;"
cursor.execute(existing_demo_group_query)
for row in cursor:
    existing_demo_groups[row['gender']+row['age']] = row['id']

existing_pages = {}
existing_pages_query = "select page_id, page_name from pages;"
cursor.execute(existing_pages_query)
for row in cursor:
    existing_pages[row['page_id']] = row['page_name']

print(existing_pages)

#data structures to hold new ads
AdRecord = namedtuple('AdRecord', ['archive_id', 
                                   'page_id', 
                                   'image_url', 
                                   'creation_date', 
                                   'start_date', 
                                   'end_date', 
                                   'is_active', 
                                   'min_impressions', 
                                   'max_impressions', 
                                   'min_spend', 
                                   'max_spend'])
PageRecord = namedtuple('PageRecord', ['id', 'name'])
#ImpressionsRecord = namedtuple('ImpressionsRecord', ['ad_archive_id', 'crawl_date', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend'])
SnapshotRegionRecord = namedtuple('SnapshotRegionRecord', ['name', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend', 'crawl_date'])
SnapshotDemoRecord = namedtuple('SnapshotDemoRecord', ['age_range', 'gender', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend', 'crawl_date'])
new_ads = set()
new_pages = set()
new_demo_groups = {}
new_regions = set()
new_impressions = set()
new_ad_region_impressions = set()
new_ad_demo_impressions = set()

#get ads
FB_ACCESS_TOKEN = config['FACEBOOK']['TOKEN']
graph = facebook.GraphAPI(access_token=FB_ACCESS_TOKEN)
field_list = ["ad_creation_time","ad_delivery_start_time","ad_delivery_stop_time","ad_snapshot_url", "currency", "demographic_distribution", "impressions", "page_id", "page_name", "region_distribution", "spend"]
with open(config['SEARCH']['TERM_FILE']) as term_file:
    for term in term_file:
        has_next = True
        next_cursor = ""
        print(term.strip())
        while has_next:
            try:
                if not next_cursor:
                    sleep(3)
                    results = graph.get_object(id='ads_archive', 
                                               ad_reached_countries='US', 
                                               ad_type='POLITICAL_AND_ISSUE_ADS',
                                               ad_active_status='ALL',
                                               search_terms=term.strip(),
                                               fields=",".join(field_list))
                else:
                    sleep(3)
                    results = graph.get_object(id='ads_archive', 
                                               ad_reached_countries='US', 
                                               ad_type='POLITICAL_AND_ISSUE_ADS',
                                               ad_active_status='ALL',
                                               search_terms=term.strip(),
                                               fields=",".join(field_list),
                                               after=next_cursor)
            except facebook.GraphAPIError as e:
                print(e)
                if e.code == 4: # this means we've gotten to the FB max results per query
                    has_next = False
                    continue

                
            for result in results['data']:
                image_url = result['ad_snapshot_url']
                url_parts = urlparse(image_url)
                archive_id = parse_qs(url_parts.query)['id'][0]
                page_id = result['page_id']
                page_name = result['page_name']
                start_date = result['ad_delivery_start_time']
                end_date = ""
                if 'ad_delivery_stop_time' in result:
                    end_date = result['ad_delivery_stop_time']
                if 'impressions' not in result:
                    print("no impressions for " + str(archive_id))
                    continue

                min_impressions = result['impressions']['lower_bound']
                max_impressions = result['impressions']['upper_bound']
                min_spend = result['spend']['lower_bound']
                max_spend = result['spend']['upper_bound']
                is_active = True
                if int(page_id) not in existing_pages:
                    new_pages.add(PageRecord(page_id, page_name))

                if end_date:
                    is_active = False

                curr_ad = AdRecord(archive_id, 
                                   page_id, 
                                   image_url, 
                                   start_date, 
                                   start_date, 
                                   end_date, 
                                   is_active, 
                                   min_impressions, 
                                   max_impressions, 
                                   min_spend, 
                                   max_spend)

                if is_active or archive_id in active_ads or archive_id not in ad_ids:
                    new_impressions.add(curr_ad)
                    for demo_result in result['demographic_distribution']:
                        demo_key = demo_result['gender']+demo_result['age']
                        if demo_key not in existing_demo_groups:
                            new_demo_groups[demo_key] = (demo_result['gender'], demo_result['age'])

                        new_ad_demo_impressions.add(SnapshotDemoRecord(demo_result['age'], 
                                                                       demo_result['gender'], 
                                                                       float(demo_result['percentage']) * int(min_impressions), 
                                                                       float(demo_result['percentage']) * int(max_impressions), 
                                                                       float(demo_result['percentage']) * int(min_spend), 
                                                                       float(demo_result['percentage']) * int(max_spend), 
                                                                       crawl_date))
                        
                    for region_result in result['region_distribution']:
                        if region_result['region'] not in new_regions:
                            new_regions.add(region_result['region'])
                        new_ad_region_impressions.add(SnapshotRegionRecord(region_result['region'], 
                                                                           float(region_result['percentage']) * int(min_impressions), 
                                                                           float(region_result['percentage']) * int(max_impressions), 
                                                                           float(region_result['percentage']) * int(min_spend), 
                                                                           float(region_result['percentage']) * int(max_spend), 
                                                                           crawl_date))

                if archive_id not in ad_ids:
                    new_ads.add(curr_ad)
                    ad_ids.add(archive_id)


            if "next" in results["paging"]:
                next_cursor = results["paging"]["cursors"]["after"]
            else:
                has_next = False


#write new pages, regions, and demo groups to db first so we can update our caches before writing ads
insert_page = "INSERT INTO pages(page_id, page_name) VALUES "
page_count = 0
for page in new_pages:
    insert_page += cursor.mogrify("(%s, %s),",(page.id, page.name)).decode('utf-8')
    page_count += 1
    
    if page_count >= 250:
        insert_page = insert_page[:-1]
        insert_page += ";"
        print(cursor.mogrify(insert_page))
        cursor.execute(insert_page)
        insert_page = "INSERT INTO pages(page_id, page_name) VALUES "
        page_count = 0

insert_page = insert_page[:-1]
insert_page += ";"
if page_count > 0:
    print(cursor.mogrify(insert_page))
    cursor.execute(insert_page)

insert_regions = "INSERT into regions(name) VALUES "
region_count = 0
for region in new_regions:
    insert_regions += cursor.mogrify("(%s),",(region,)).decode('utf-8')
    region_count += 1
    
    if region_count >= 250:
        insert_regions = insert_regions[:-1]
        insert_region += ";"
        print(cursor.mogrify(insert_regions))
        cursor.execute(insert_region)
        insert_regions = "INSERT INTO regions(name) VALUES "
        region_count = 0

if region_count > 0:
    insert_regions = insert_regions[:-1]
    insert_regions += ";"
    print(cursor.mogrify(insert_regions))
    cursor.execute(insert_regions)

insert_demo_groups = "INSERT INTO demo_groups(age, gender) VALUES "
demo_group_count = 0
for key, val in new_demo_groups.items():
    insert_demo_groups += cursor.mogrify("(%s, %s),",(val[0], val[1])).decode('utf-8')
    demo_group_count += 1
    
    if demo_group_count >= 250:
        insert_demo_groups = insert_demo_groups[:-1]
        insert_demo_groups += ";"
        print(cursor.mogrify(insert_demo_groups))
        cursor.execute(insert_demo_groups)
        insert_demo_groups = "INSERT INTO demo_groups(age, gender) VALUES "
        demo_group_count = 0

if demo_group_count > 0:
    insert_demo_groups = insert_demo_groups[:-1]
    insert_demo_groups += ";"
    print(cursor.mogrify(insert_demo_groups))
    cursor.execute(insert_demo_groups)

#cursor.commit()

existing_regions = {}
existing_regions_query = "select name, id from regions;"
cursor.execute(existing_regions_query)
for row in cursor:
    existing_regions[row['name']] = row['id']

existing_demo_groups = {}
existing_demo_group_query = "select gender, age, id from demo_groups;"
cursor.execute(existing_demo_group_query)
for row in cursor:
    existing_demo_groups[row['gender']+row['age']] = row['id']

existing_pages = {}
existing_pages_query = "select page_id, page_name from pages;"
cursor.execute(existing_pages_query)
for row in cursor:
    existing_pages[row['page_id']] = row['page_name']

#write new ads to our database
ad_insert_query = "INSERT INTO ads(id, creation_date, start_date, end_date, currency, page_id, snapshot_url, text, is_active) VALUES "
ad_count = 0
for ad in new_ads:

    ad_insert_query += cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s),", (ad.archive_id, ad.creation_date, ad.start_date, ad.end_date, "", ad.page_id, "", "", is_active)).decode('utf-8')
    ad_count += 1

    if ad_count >= 250:
        ad_insert_query = ad_insert_query[:-1]
        ad_insert_query += ";"
        print(cursor.mogrify(ad_insert_query))
        cursor.execute(ad_insert_query)
        ad_insert_query = "INSERT INTO ads(id, creation_date, start_date, end_date, currency, page_id, snapshot_url, text, is_active) VALUES "
        ad_count = 0

if ad_count > 0:
    ad_insert_query = ad_insert_query[:-1]
    ad_insert_query += ";"
    print(cursor.mogrify(ad_insert_query))
    cursor.execute(ad_insert_query)


impressions_insert_query = "INSERT INTO impressions(ad_archive_id, crawl_date, min_impressions, min_spend, max_impressions, max_spend) VALUES "
impression_count = 0
for impression in new_impressions:
    impressions_insert_query += cursor.mogrify("(%s, %s, %s, %s, %s, %s),", (impression.archive_id, curr_date, impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend))
    impression_count += 1

    if impression_count >= 250:
        impressions_insert_query = impressions_insert_query[:-1]
        impressions_insert_query += ";"
        print(cursor.mogrify(impressions_insert_query))
        cursor.execute(impressions_insert_query)
        impressions_insert_query = "INSERT INTO impressions(ad_archive_id, crawl_date, min_impressions, min_spend, max_impressions, max_spend) VALUES "
        impression_count = 0

if impression_count > 0:
    impressions_insert_query = impressions_insert_query[:-1]
    impressions_insert_query += ";"
    print(cursor.mogrify(impressions_insert_query))
    cursor.execute(impressions_insert_query)


impression_demo_insert_query = "INSERT INTO demo_impressions(ad_archive_id, demo_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
impression_count = 0
for impression in new_ad_demo_impressions:
    impression_demo_insert_query += cursor.mogrify("(%s, %s, %s, %s, %s, %s, current_date),", (impression.archive_id, existing_demo_groups[impression.gender + impression.age], impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend))
    impression_count += 1

    if impression_count >= 250:
        impression_demo_insert_query = impression_demo_insert_query[:-1]
        impression_demo_insert_query += ";"
        print(cursor.mogrify(impression_demo_insert_query))
        cursor.execute(impression_demo_insert_query)
        impression_demo_insert_query = "INSERT INTO demo_impressions(ad_archive_id, demo_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
        impression_count = 0

if impression_count > 0:
    impression_demo_insert_query = impression_demo_insert_query[:-1]
    impression_demo_insert_query += ";"
    print(cursor.mogrify(impression_demo_insert_query))
    cursor.execute(impression_demo_insert_query)


impression_region_insert_query = "INSERT INTO region_impressions(ad_archive_id, region_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
impression_count = 0
for impression in new_ad_region_impressions:
    impression_region_insert_query += cursor.mogrify("(%s, %s, %s, %s, %s, %s, current_date),", (impression.archive_id, existing_regions[impression.name],  impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend))
    impression_count += 1

    if impression_count >= 250:
        impression_region_insert_query = impression_region_insert_query[:-1]
        impression_region_insert_query += ";"
        print(cursor.mogrify(impression_region_insert_query))
        cursor.execute(impression_region_insert_query)
        impression_region_insert_query = "INSERT INTO region_impressions(ad_archive_id, region_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
        impression_count = 0

if impression_count > 0:
    impression_region_insert_query = impression_region_insert_query[:-1]
    impression_region_insert_query += ";"
    print(cursor.mogrify(impression_region_insert_query))
    cursor.execute(impression_region_insert_query)


#cursor.commit()

connection.close()
