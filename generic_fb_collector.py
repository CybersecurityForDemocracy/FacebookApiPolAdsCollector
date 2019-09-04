from urllib.parse import urlparse, parse_qs
import json
import random
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
from OpenSSL import SSL

from db_functions import DBInterface

if len(sys.argv) < 2:
    exit("Usage:python3 generic_fb_collector.py generic_fb_collector.cfg")
config = configparser.ConfigParser()
config.read(sys.argv[1])

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
                                   'currency',
                                   'ad_creative_link_caption',
                                   'ad_creative_link_description',
                                   'ad_creative_link_title'])
PageRecord = namedtuple('PageRecord', ['id', 'name'])
SnapshotRegionRecord = namedtuple('SnapshotRegionRecord', ['archive_id', 'name', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend', 'crawl_date'])
SnapshotDemoRecord = namedtuple('SnapshotDemoRecord', ['archive_id', 'age_range', 'gender', 'min_impressions', 'max_impressions', 'min_spend', 'max_spend', 'crawl_date'])
FB_ACCESS_TOKEN = config['FACEBOOK']['TOKEN']
SLEEP_TIME = int(config['SEARCH']['SLEEP_TIME'])
field_list = ["ad_creation_time","ad_delivery_start_time","ad_delivery_stop_time","ad_snapshot_url", "currency", "demographic_distribution", "impressions", "page_id", "page_name", "region_distribution", "spend", "ad_creative_body", "funding_entity", "ad_creative_link_caption", "ad_creative_link_description", "ad_creative_link_title"]

#setup our db cursor
HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
PORT = config['POSTGRES']['PORT']
DBAuthorize = "host=%s dbname=%s user=%s password=%s port=%s" % (HOST, DBNAME, USER, PASSWORD, PORT)
connection = psycopg2.connect(DBAuthorize)



def run_search(search_term, connection, db):
    crawl_date = datetime.date.today() 
    country_code = config['SEARCH']['COUNTRY_CODE']

    #structures to hold all the new stuff we find
    new_ads = set()
    new_ad_sponsors = set()
    new_pages = set()
    new_demo_groups = {}
    new_regions = set()
    new_impressions = set()
    new_ad_region_impressions = {}
    new_ad_demo_impressions = {}

    #cache of ads/pages/regions/demo_groups we've already seen so we don't reinsert them
    (ad_ids, active_ads) = db.existing_ads()
    existing_regions = db.existing_region()
    existing_demo_groups = db.existing_demos()
    existing_pages = db.existing_page()
    existing_ad_sponsors = db.existing_sponsors()

    #get ads
    graph = facebook.GraphAPI(access_token=FB_ACCESS_TOKEN)
    has_next = True
    already_seen = False
    next_cursor = ""
    print(datetime.datetime.now())

    print(search_term)
    request_count = 0
    while has_next and not already_seen and request_count < 30:
        request_count += 1
        try:
            results = None
            if type(search_term) == str:
                print("making search term request for " + search_term)
                if not next_cursor:
                    sleep(SLEEP_TIME)
                    results = graph.get_object(id='ads_archive', 
                                               ad_reached_countries=country_code, 
                                               ad_type='POLITICAL_AND_ISSUE_ADS',
                                               ad_active_status='ALL',
                                               limit=800,
                                               search_terms=search_term,
                                               fields=",".join(field_list))
                else:
                    sleep(SLEEP_TIME * 2)
                    print("making request")
                    results = graph.get_object(id='ads_archive', 
                                               ad_reached_countries=country_code, 
                                               ad_type='POLITICAL_AND_ISSUE_ADS',
                                               ad_active_status='ALL',
                                               limit=800,
                                               search_terms=search_term,
                                               fields=",".join(field_list),
                                               after=next_cursor)
            else:
                print("making page_id request for " + str(search_term))
                if not next_cursor:
                    sleep(SLEEP_TIME)
                    print("making request")
                    results = graph.get_object(id='ads_archive', 
                                               ad_reached_countries=country_code, 
                                               ad_type='POLITICAL_AND_ISSUE_ADS',
                                               ad_active_status='ALL',
                                               limit=800,
                                               search_page_ids=search_term,
                                               fields=",".join(field_list))
                else:
                    sleep(SLEEP_TIME * 2)
                    print("making request")
                    results = graph.get_object(id='ads_archive', 
                                               ad_reached_countries=country_code, 
                                               ad_type='POLITICAL_AND_ISSUE_ADS',
                                               ad_active_status='ALL',
                                               limit=800,
                                               search_page_ids=search_term,
                                               fields=",".join(field_list),
                                               after=next_cursor)
        except facebook.GraphAPIError as e:
            print("Graph Error")
            print(e.code)
            print(e)
            if results:
                print(results)
            else:
                print("No results")
            if e.code == 4: # this means we've gotten to the FB max results per query
                sleep(240)
                has_next = False
                continue
            else:
                print("resetting graph")
                graph = facebook.GraphAPI(access_token=FB_ACCESS_TOKEN)
                continue
        except OSError as e:
            print("OS error: {0}".format(e))
            print(datetime.datetime.now())
            sleep(60)
            print("resetting graph")
            graph = facebook.GraphAPI(access_token=FB_ACCESS_TOKEN)
            continue

        except SSL.SysCallError as e:
            print("resetting graph")
            graph = facebook.GraphAPI(access_token=FB_ACCESS_TOKEN)
            continue


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

            is_active = True
            end_date = None
            if 'ad_delivery_stop_time' in result:
                end_date = result['ad_delivery_stop_time']
                is_active = False

            min_impressions = 0
            max_impressions = 0
            min_spend = 0
            max_spend = 0
            if 'impressions' in result:
                min_impressions = result['impressions']['lower_bound']
                max_impressions = result['impressions']['upper_bound']
            if 'spend' in result:
                min_spend = result['spend']['lower_bound']
                max_spend = result['spend']['upper_bound']

            link_caption = ''
            if 'ad_creative_link_caption' in result:
                link_caption = result['ad_creative_link_caption']
            link_description = ''
            if 'ad_creative_link_description' in result:
                link_description = result['ad_creative_link_description']
            link_title = ''
            if 'ad_creative_link_title' in result:
                link_description = result['ad_creative_link_title']

            if int(page_id) not in existing_pages:
                new_pages.add(PageRecord(page_id, page_name))

            if not is_active and archive_id in ad_ids:
                    old_ad_count += 1

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
                               currency,
                               link_caption,
                               link_description,
                               link_title)


            if is_active or archive_id in active_ads or archive_id not in ad_ids:
                new_impressions.add(curr_ad)
                if 'demographic_distribution' not in result:
                    print("no demo information in:")
                    print(result)
                    continue
                   
                if 'region_distribution' not in result:
                    print("no region information in:")
                    print(result)
                    continue
                   
                for demo_result in result['demographic_distribution']:
                    demo_key = demo_result['gender']+demo_result['age']
                    if demo_key not in existing_demo_groups:
                        new_demo_groups[demo_key] = (demo_result['gender'], demo_result['age'])

                    if archive_id in new_ad_demo_impressions:
                        if demo_result['age'] + demo_result['gender'] not in new_ad_demo_impressions[archive_id]:
                            new_ad_demo_impressions[archive_id][demo_result['age'] + demo_result['gender']] = SnapshotDemoRecord(archive_id,
                                                                   demo_result['age'], 
                                                                   demo_result['gender'], 
                                                                   float(demo_result['percentage']) * int(min_impressions), 
                                                                   float(demo_result['percentage']) * int(max_impressions), 
                                                                   float(demo_result['percentage']) * int(min_spend), 
                                                                   float(demo_result['percentage']) * int(max_spend), 
                                                                   crawl_date)
                    else:
                        new_ad_demo_impressions[archive_id] = {demo_result['age'] + demo_result['gender']: SnapshotDemoRecord(archive_id,
                                                                   demo_result['age'], 
                                                                   demo_result['gender'], 
                                                                   float(demo_result['percentage']) * int(min_impressions), 
                                                                   float(demo_result['percentage']) * int(max_impressions), 
                                                                   float(demo_result['percentage']) * int(min_spend), 
                                                                   float(demo_result['percentage']) * int(max_spend), 
                                                                   crawl_date)}
                    
                for region_result in result['region_distribution']:
                    if region_result['region'] not in existing_regions:
                        new_regions.add(region_result['region'])
                    if archive_id in new_ad_region_impressions:
                        if region_result['region'] not in new_ad_region_impressions[archive_id]:
                            new_ad_region_impressions[archive_id][region_result['region']] = SnapshotRegionRecord(archive_id,
                                                                       region_result['region'], 
                                                                       float(region_result['percentage']) * int(min_impressions), 
                                                                       float(region_result['percentage']) * int(max_impressions), 
                                                                       float(region_result['percentage']) * int(min_spend), 
                                                                       float(region_result['percentage']) * int(max_spend), 
                                                                       crawl_date)


                    else:
                        new_ad_region_impressions[archive_id] = {region_result['region']: SnapshotRegionRecord(archive_id,
                                                                       region_result['region'], 
                                                                       float(region_result['percentage']) * int(min_impressions), 
                                                                       float(region_result['percentage']) * int(max_impressions), 
                                                                       float(region_result['percentage']) * int(min_spend), 
                                                                       float(region_result['percentage']) * int(max_spend), 
                                                                       crawl_date)}

            if archive_id not in ad_ids:
                new_ads.add(curr_ad)
                ad_ids.add(archive_id)


        #we finished parsing each result
        print(old_ad_count)
        print("total ads=" + str(total_ad_count))
        if total_ad_count > 0 and float(old_ad_count) / float(total_ad_count) > .75:
            already_seen = True

        if "paging" in results and "next" in results["paging"]:
            next_cursor = results["paging"]["cursors"]["after"]
        else:
            has_next = False

    #write new pages, regions, and demo groups to db first so we can update our caches before writing ads
    db.insert_ad_sponsors(new_ad_sponsors)
    db.insert_pages(new_pages)
    db.insert_regions(new_regions)
    db.insert_demos(new_demo_groups)

    connection.commit()
    existing_regions = db.existing_region()
    existing_demo_groups = db.existing_demos()
    existing_pages = db.existing_page()
    existing_ad_sponsors = db.existing_sponsors()

    #write new ads to our database
    print("writing " + str(len(new_ads)) + " to db")
    db.write_ads_to_db(new_ads, country_code, currency, existing_ad_sponsors)

    print("writing " + str(len(new_impressions)) + " impressions to db")
    db.write_impressions_to_db(new_impressions, crawl_date)

    print("writing new_ad_demo_impressions to db")
    db.write_demo_impressions_to_db(new_impressions, crawl_date, existing_demo_groups)

    print("writing new_ad_region_impressions to db")
    db.write_region_impressions_to_db(new_ad_region_impressions, existing_regions)
    connection.commit()



#get page data
def get_page_data(input_connection):
    page_ids = set()
    page_names = set()
    input_TYPE = config['INPUT']['TYPE']
    if input_TYPE == 'file':
        input_FILES = config['INPUT']['FILES']
    print(input_FILES)
    file_list = json.loads(input_FILES)
    for file_name in file_list:
        with open(file_name) as input:
            for row in input:
                page_ids.add(row.strip())
    else:
        input_cursor = input_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        general_table = config['INPUT']['GENERAL_TABLE']
        weekly_table = config['INPUT']['WEEKLYTABLE']
        search_limit = config['SEARCH']['LIMIT']
        for row in input_cursor:
            this_week_pages_query = f'select page_name, {general_table}.fb_id, total_ads from {weekly_table} join {general_table} \
            on {weekly_table}.nyu_id = {general_table}.nyu_id \
            where week in \
            (select max(week) from {weekly_table}) \
            order by total_ads desc \
            limit({search_limit});'
            input_cursor.execute(this_week_pages_query)
            for row in input_cursor:
                if row['fb_id']:
                    page_ids.add(int(row['fb_id']))
                else:
                    page_names.add(row['page_name'])
    return page_ids, page_names



def main():
    db = DBInterface(connection)
    page_ids, page_names = get_page_data(connection)
    all_ids = list(page_ids)
    random.shuffle(all_ids)
    print(f"Page ID count: {len(all_ids)}")
    print(f"Page Name count: {len(page_names)}")
    for page_id in all_ids:
        run_search(page_id, connection, db)

    for page in page_names:
        run_search(page, connection, db)
    connection.close()


if __name__ == '__main__':
    main()