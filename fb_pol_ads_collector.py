from urllib.parse import urlparse, parse_qs
import configparser
from collections import namedtuple
from time import sleep
import datetime
import time
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

def existing_ads(cursor):
    existing_ad_query = "select archive_id, is_active from ads"
    cursor.execute(existing_ad_query)
    ad_ids = set()
    active_ads = set()
    for row in cursor:
        ad_ids.add(row['archive_id'])
        if row['is_active'] == True:
            active_ads.add(row['archive_id'])
    
    return (ad_ids, active_ads)

def existing_demos(cursor):
    existing_demo_group_query = "select gender, age, id from demo_groups;"
    cursor.execute(existing_demo_group_query)
    existing_demo_groups = {}
    for row in cursor:
        existing_demo_groups[row['gender']+row['age']] = row['id']

    return existing_demo_groups

def existing_region(cursor):
    existing_regions_query = "select name, id from regions;"
    cursor.execute(existing_regions_query)
    existing_regions = {}
    for row in cursor:
        existing_regions[row['name']] = row['id']

    return existing_regions

def existing_page(cursor):
    existing_pages_query = "select page_id, page_name from pages;"
    cursor.execute(existing_pages_query)
    existing_pages = {}
    for row in cursor:
        existing_pages[row['page_id']] = row['page_name']

    return existing_pages

def existing_sponsors(cursor):
    existing_ad_sponsor_query = "select id, name from ad_sponsors;"
    cursor.execute(existing_ad_sponsor_query)
    existing_ad_sponsors = {}
    for row in cursor:
        existing_ad_sponsors[row['name']] = row['id']

    return existing_ad_sponsors

def insert_ad_sponsors(cursor, new_ad_sponsors):
    insert_ad_sponsor = "INSERT INTO ad_sponsors(name) VALUES "
    ad_sponsor_count = 0
    for ad_sponsor in new_ad_sponsors:
        insert_ad_sponsor += cursor.mogrify("(%s),", (ad_sponsor,)).decode('utf-8')
        ad_sponsor_count += 1

        if ad_sponsor_count >= 250:
            insert_ad_sponsor = insert_ad_sponsor[:-1]
            insert_ad_sponsor += ";"
            #print(cursor.mogrify(insert_ad_sponsor))
            cursor.execute(insert_ad_sponsor)
            insert_ad_sponsor = "INSERT INTO ad_sponsors(name) VALUES "
            ad_sponsor_count = 0

    if ad_sponsor_count > 0:
        insert_ad_sponsor = insert_ad_sponsor[:-1]
        insert_ad_sponsor += ";"
        #print(cursor.mogrify(insert_ad_sponsor))
        cursor.execute(insert_ad_sponsor)


def insert_pages(cursor, new_pages):
    insert_page = "INSERT INTO pages(page_id, page_name) VALUES "
    page_count = 0
    for page in new_pages:
        insert_page += cursor.mogrify("(%s, %s),",(page.id, page.name)).decode('utf-8')
        page_count += 1
    
        if page_count >= 250:
            insert_page = insert_page[:-1]
            insert_page += ";"
            #print(cursor.mogrify(insert_page))
            cursor.execute(insert_page)
            insert_page = "INSERT INTO pages(page_id, page_name) VALUES "
            page_count = 0

    insert_page = insert_page[:-1]
    insert_page += ";"
    if page_count > 0:
        #print(cursor.mogrify(insert_page))
        cursor.execute(insert_page)

def insert_regions(cursor, new_regions):
    insert_region = "INSERT into regions(name) VALUES "
    region_count = 0
    for region in new_regions:
        insert_region += cursor.mogrify("(%s),",(region,)).decode('utf-8')
        region_count += 1
    
        if region_count >= 250:
            insert_region = insert_region[:-1]
            insert_region += ";"
            cursor.execute(insert_region)
            insert_region = "INSERT INTO regions(name) VALUES "
            region_count = 0

    if region_count > 0:
        insert_region = insert_region[:-1]
        insert_region += ";"
        #print(cursor.mogrify(insert_regions))
        cursor.execute(insert_region)

def insert_demos(cursor, new_demo_groups):
    insert_demo_groups = "INSERT INTO demo_groups(age, gender) VALUES "
    demo_group_count = 0
    for key, val in new_demo_groups.items():
        insert_demo_groups += cursor.mogrify("(%s, %s),",(val[0], val[1])).decode('utf-8')
        demo_group_count += 1
    
        if demo_group_count >= 250:
            insert_demo_groups = insert_demo_groups[:-1]
            insert_demo_groups += ";"
            #print(cursor.mogrify(insert_demo_groups))
            cursor.execute(insert_demo_groups)
            insert_demo_groups = "INSERT INTO demo_groups(age, gender) VALUES "
            demo_group_count = 0

    if demo_group_count > 0:
        insert_demo_groups = insert_demo_groups[:-1]
        insert_demo_groups += ";"
        #print(cursor.mogrify(insert_demo_groups))
        cursor.execute(insert_demo_groups)



#get page data
HOST = config['INPUT']['HOST']
DBNAME = config['INPUT']['DBNAME']
USER = config['INPUT']['USER']
PASSWORD = config['INPUT']['PASSWORD']
PORT = config['INPUT']['PORT']
DBAuthorize = "host=%s dbname=%s user=%s password=%s port=%s" % (HOST, DBNAME, USER, PASSWORD, PORT)
input_connection = psycopg2.connect(DBAuthorize)
input_cursor = input_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
page_query = "SELECT id from pages ORDER BY id"
input_cursor.execute(page_query)
page_ids = []
for row in input_cursor:
    page_ids.append(row['id'])

CLUSTER_SIZE = config['SEARCH']['CLUSTER_SIZE']
NODE = config['SEARCH']['NODE']
all_terms = page_ids[int(NODE)::int(CLUSTER_SIZE)]

#setup our db cursor
HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
PORT = config['POSTGRES']['PORT']
DBAuthorize = "host=%s dbname=%s user=%s password=%s port=%s" % (HOST, DBNAME, USER, PASSWORD, PORT)
connection = psycopg2.connect(DBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#data structures to hold new ads
AdRecord = namedtuple('AdRecord', ['archive_id', 
                                   'page_id', 
                                   'image_url', 
                                   'text',
                                   'sponsor_label',
                                   'creation_date', 
                                   'start_date', 
                                   'end_date', 
                                   'is_active', 
                                   'min_impressions', 
                                   'max_impressions', 
                                   'min_spend', 
                                   'max_spend',
                                   'currency'])
PageRecord = namedtuple('PageRecord', ['id', 'name'])
SnapshotRegionRecord = namedtuple('SnapshotRegionRecord', ['archive_id', 'name', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend', 'crawl_date'])
SnapshotDemoRecord = namedtuple('SnapshotDemoRecord', ['archive_id', 'age_range', 'gender', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend', 'crawl_date'])
FB_ACCESS_TOKEN = config['FACEBOOK']['TOKEN']
field_list = ["ad_creation_time","ad_delivery_start_time","ad_delivery_stop_time","ad_snapshot_url", "currency", "demographic_distribution", "impressions", "page_id", "page_name", "region_distribution", "spend", "ad_creative_body", "funding_entity"]
#OLDEST_DATE = datetime.datetime.strptime(config['SEARCH']['OLDEST_DATE'], '%Y-%m-%d')

while all_terms:
    term_list = all_terms[:100]
    all_terms = all_terms[100:]

    #structures to hold all the new stuff we find
    new_ads = set()
    new_ad_sponsors = set()
    new_pages = set()
    new_demo_groups = {}
    new_regions = set()
    new_impressions = set()
    new_ad_region_impressions = set()
    new_ad_demo_impressions = set()

    #cache of ads/pages/regions/demo_groups we've already seen so we don't reinsert them
    (ad_ids, active_ads) = existing_ads(cursor)
    existing_regions = existing_region(cursor)
    existing_demo_groups = existing_demos(cursor)
    existing_pages = existing_page(cursor)
    existing_ad_sponsors = existing_sponsors(cursor)

    #get ads
    graph = facebook.GraphAPI(access_token=FB_ACCESS_TOKEN)
    print(term_list)
    for term in term_list:
        has_next = True
        already_seen = False
        next_cursor = ""
        print(term)
        while has_next and not already_seen:
            try:
                if not next_cursor:
                    sleep(16)
                    results = graph.get_object(id='ads_archive', 
                                               ad_reached_countries='US', 
                                               ad_type='POLITICAL_AND_ISSUE_ADS',
                                               ad_active_status='ALL',
                                               limit=5000,
                                               search_terms=term,
                                               fields=",".join(field_list))
                else:
                    sleep(15)
                    results = graph.get_object(id='ads_archive', 
                                               ad_reached_countries='US', 
                                               ad_type='POLITICAL_AND_ISSUE_ADS',
                                               ad_active_status='ALL',
                                               limit=5000,
                                               search_terms=term,
                                               fields=",".join(field_list),
                                               after=next_cursor)
            except facebook.GraphAPIError as e:
                print(e)
                if e.code == 4: # this means we've gotten to the FB max results per query
                    has_next = False
                    continue
                else:
                    graph = facebook.GraphAPI(access_token=FB_ACCESS_TOKEN)

            old_ad_count = 0 
            total_ad_count = 0
            for result in results['data']:
                total_ad_count += 1
                image_url = result['ad_snapshot_url']
                url_parts = urlparse(image_url)
                archive_id = int(parse_qs(url_parts.query)['id'][0])
                page_id = result['page_id']
                page_name = result['page_name']
                start_date = result['ad_delivery_start_time']
                currency = result['currency']
                ad_text = ''
                if 'ad_creative_body' in result:
                    ad_text = result['ad_creative_body']
                ad_sponsor_label = ''
                if 'funding_entity' in result:
                    ad_sponsor_label = result['funding_entity']

                if ad_sponsor_label not in existing_ad_sponsors:
                    new_ad_sponsors.add(ad_sponsor_label)

                end_date = None
                if 'ad_delivery_stop_time' in result:
                    end_date = result['ad_delivery_stop_time']
                if 'impressions' not in result or 'spend' not in result:
                    print("no impressions or no spend for " + str(archive_id))
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

                parsed_end_date = None
                if end_date:
                    parsed_end_date = datetime.datetime.strptime(end_date[:10], '%Y-%m-%d')
                parsed_start_date = None
                if start_date:
                    datetime.datetime.strptime(start_date[:10], '%Y-%m-%d')

                #if parsed_end_date and parsed_end_date < OLDEST_DATE:
                #    old_ad_count += 1
                #else:
                #    if not is_active:
                #        if archive_id in ad_ids or ( parsed_start_date and parsed_start_date < OLDEST_DATE):
                #            old_ad_count += 1

                curr_ad = AdRecord(archive_id, 
                                   page_id, 
                                   image_url, 
                                   ad_text,
                                   ad_sponsor_label,
                                   start_date, 
                                   start_date, 
                                   end_date, 
                                   is_active, 
                                   min_impressions, 
                                   max_impressions, 
                                   min_spend, 
                                   max_spend,
                                   currency)

                if is_active or archive_id in active_ads or archive_id not in ad_ids:
                    new_impressions.add(curr_ad)
                    if 'demographic_distribution' not in result:
                        print("no demo information in:")
                        print(result)
                        continue
                       
                    for demo_result in result['demographic_distribution']:
                        demo_key = demo_result['gender']+demo_result['age']
                        if demo_key not in existing_demo_groups:
                            new_demo_groups[demo_key] = (demo_result['gender'], demo_result['age'])

                        new_ad_demo_impressions.add(SnapshotDemoRecord(archive_id,
                                                                       demo_result['age'], 
                                                                       demo_result['gender'], 
                                                                       float(demo_result['percentage']) * int(min_impressions), 
                                                                       float(demo_result['percentage']) * int(max_impressions), 
                                                                       float(demo_result['percentage']) * int(min_spend), 
                                                                       float(demo_result['percentage']) * int(max_spend), 
                                                                       crawl_date))
                        
                    for region_result in result['region_distribution']:
                        if region_result['region'] not in new_regions:
                            new_regions.add(region_result['region'])
                        new_ad_region_impressions.add(SnapshotRegionRecord(archive_id,
                                                                           region_result['region'], 
                                                                           float(region_result['percentage']) * int(min_impressions), 
                                                                           float(region_result['percentage']) * int(max_impressions), 
                                                                           float(region_result['percentage']) * int(min_spend), 
                                                                           float(region_result['percentage']) * int(max_spend), 
                                                                           crawl_date))

                if archive_id not in ad_ids:
                    new_ads.add(curr_ad)
                    ad_ids.add(archive_id)


            #we finished parsing each result
            #if total_ad_count > 0 and float(old_ad_count) / float(total_ad_count) > .75:
            #    has_next = False

            if "next" in results["paging"]:
                next_cursor = results["paging"]["cursors"]["after"]
            else:
                has_next = False

    #write new pages, regions, and demo groups to db first so we can update our caches before writing ads
    insert_ad_sponsors(cursor, new_ad_sponsors)
    insert_pages(cursor, new_pages)
    insert_regions(cursor, new_regions)
    insert_demos(cursor, new_demo_groups)

    connection.commit()
    existing_regions = existing_region(cursor)
    existing_demo_groups = existing_demos(cursor)
    existing_pages = existing_page(cursor)
    existing_ad_sponsors = existing_sponsors(cursor)

    #write new ads to our database
    print("writing to db")
    ad_insert_query = "INSERT INTO ads(archive_id, creation_date, start_date, end_date, currency, page_id, snapshot_url, text, ad_sponsor_id, is_active) VALUES "
    ad_count = 0
    for ad in new_ads:
        ad_insert_query += cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s),", (ad.archive_id, ad.creation_date, ad.start_date, ad.end_date, currency, ad.page_id, ad.image_url, ad.text, existing_ad_sponsors[ad.sponsor_label], ad.is_active)).decode('utf-8')
        ad_count += 1

        if ad_count >= 250:
            ad_insert_query = ad_insert_query[:-1]
            ad_insert_query += ";"
            #print(cursor.mogrify(ad_insert_query))
            cursor.execute(ad_insert_query)
            ad_insert_query = "INSERT INTO ads(archive_id, creation_date, start_date, end_date, currency, page_id, snapshot_url, text, ad_sponsor_id, is_active) VALUES "
            ad_count = 0

    if ad_count > 0:
        ad_insert_query = ad_insert_query[:-1]
        ad_insert_query += ";"
        #print(cursor.mogrify(ad_insert_query))
        cursor.execute(ad_insert_query)

    impressions_insert_query = "INSERT INTO impressions(ad_archive_id, crawl_date, min_impressions, min_spend, max_impressions, max_spend) VALUES "
    impression_count = 0
    for impression in new_impressions:
        impressions_insert_query += cursor.mogrify("(%s, %s, %s, %s, %s, %s),", (impression.archive_id, crawl_date, impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend)).decode('utf-8')
        impression_count += 1

        if impression_count >= 250:
            impressions_insert_query = impressions_insert_query[:-1]
            impressions_insert_query += ";"
            #print(cursor.mogrify(impressions_insert_query))
            cursor.execute(impressions_insert_query)
            impressions_insert_query = "INSERT INTO impressions(ad_archive_id, crawl_date, min_impressions, min_spend, max_impressions, max_spend) VALUES "
            impression_count = 0

    if impression_count > 0:
        impressions_insert_query = impressions_insert_query[:-1]
        impressions_insert_query += ";"
        #print(cursor.mogrify(impressions_insert_query))
        cursor.execute(impressions_insert_query)

    impression_demo_insert_query = "INSERT INTO demo_impressions(ad_archive_id, demo_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
    impression_count = 0
    for impression in new_ad_demo_impressions:
        impression_demo_insert_query += cursor.mogrify("(%s, %s, %s, %s, %s, %s, current_date),", (impression.archive_id, existing_demo_groups[impression.age_range + impression.gender], impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend)).decode('utf-8')
        impression_count += 1

        if impression_count >= 250:
            impression_demo_insert_query = impression_demo_insert_query[:-1]
            impression_demo_insert_query += ";"
            #print(cursor.mogrify(impression_demo_insert_query))
            cursor.execute(impression_demo_insert_query)
            impression_demo_insert_query = "INSERT INTO demo_impressions(ad_archive_id, demo_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
            impression_count = 0

    if impression_count > 0:
        impression_demo_insert_query = impression_demo_insert_query[:-1]
        impression_demo_insert_query += ";"
        #print(cursor.mogrify(impression_demo_insert_query))
        cursor.execute(impression_demo_insert_query)

    impression_region_insert_query = "INSERT INTO region_impressions(ad_archive_id, region_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
    impression_count = 0
    for impression in new_ad_region_impressions:
        impression_region_insert_query += cursor.mogrify("(%s, %s, %s, %s, %s, %s, current_date),", (impression.archive_id, existing_regions[impression.name],  impression.min_impressions, impression.min_spend, impression.max_impressions, impression.max_spend)).decode('utf-8')
        impression_count += 1

        if impression_count >= 250:
            impression_region_insert_query = impression_region_insert_query[:-1]
            impression_region_insert_query += ";"
            #print(cursor.mogrify(impression_region_insert_query))
            cursor.execute(impression_region_insert_query)
            impression_region_insert_query = "INSERT INTO region_impressions(ad_archive_id, region_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES "
            impression_count = 0

    if impression_count > 0:
        impression_region_insert_query = impression_region_insert_query[:-1]
        impression_region_insert_query += ";"
        #print(cursor.mogrify(impression_region_insert_query))
        cursor.execute(impression_region_insert_query)

    connection.commit()

connection.close()
