import psycopg2
import psycopg2.extras


class DBInterface():
    def __init__(self, connection):
        self.connection = connection

    def get_cursor(self):
        return self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def existing_ads(self):
        cursor = self.get_cursor()
        existing_ad_query = "select archive_id, ad_delivery_stop_time from ads"
        cursor.execute(existing_ad_query)
        ads_to_end_time_map = dict()
        for row in cursor:
            ads_to_end_time_map[row['archive_id']]=row['ad_delivery_stop_time']
        return ads_to_end_time_map

    def existing_pages(self):
        cursor = self.get_cursor()
        existing_pages_query = "select page_id, page_name from pages;"
        cursor.execute(existing_pages_query)
        existing_pages = set()
        for row in cursor:
            existing_pages.add(row['page_id'])
        return existing_pages

    def existing_funding_entities(self):
        cursor = self.get_cursor()
        existing_funder_query = "select funder_id, funder_name from funder_metadata;"
        cursor.execute(existing_funder_query)
        existing_funders = dict()
        for row in cursor:
            existing_funders[row['funder_name']] = row['funder_id']
        return existing_funders

    def insert_funding_entities(self, new_funders):
        cursor = self.get_cursor()
        insert_funder_query = "INSERT INTO funder_metadata(funder_name) VALUES %s;"
        insert_template = "(%s)"
        psycopg2.extras.execute_values(
            cursor, insert_funder_query, new_funders, template=insert_template, page_size=500)

    def insert_pages(self, new_pages):
        cursor = self.get_cursor()
        insert_page_query = (
            "INSERT INTO pages(page_id, page_name, page_url) VALUES (%s) "
            "on conflict (page_id) do nothing;")
        insert_template = "(%(id)s, %(name)s, https://www.facebook.com/%(id)s),",
        new_page_list = [x._asdict() for x in new_pages]
        psycopg2.extras.execute_values(
            cursor, insert_page_query, new_page_list, template=insert_template, page_size=500)

    def insert_regions(self, new_regions):
        cursor = self.get_cursor()
        insert_region = "INSERT into regions(name) VALUES "
        region_count = 0
        for region in new_regions:
            insert_region += cursor.mogrify("(%s),", (region,)).decode('utf-8')
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
            # print(self.mogrify(insert_regions))
            cursor.execute(insert_region)

    def insert_demos(self, new_demo_groups):
        cursor = self.get_cursor()
        insert_demo_groups = "INSERT INTO demo_groups(age, gender) VALUES "
        demo_group_count = 0
        for unused_key, val in new_demo_groups.items():
            insert_demo_groups += cursor.mogrify(
                "(%s, %s),", (val[0], val[1])).decode('utf-8')
            demo_group_count += 1

            if demo_group_count >= 250:
                insert_demo_groups = insert_demo_groups[:-1]
                insert_demo_groups += ";"
                # print(self.mogrify(insert_demo_groups))
                cursor.execute(insert_demo_groups)
                insert_demo_groups = "INSERT INTO demo_groups(age, gender) VALUES "
                demo_group_count = 0

        if demo_group_count > 0:
            insert_demo_groups = insert_demo_groups[:-1]
            insert_demo_groups += ";"
            # print(self.mogrify(insert_demo_groups))
            cursor.execute(insert_demo_groups)

    def insert_new_ads(self, new_ads):
        cursor = self.get_cursor()
        ad_insert_query = (
            "INSERT INTO ads(archive_id, ad_creative_body, ad_creation_time, ad_delivery_stop_time, page_id, "
            "currency, ad_creative_link_caption, ad_creative_link_title, ad_creative_link_description, "
            "ad_snapshot_url, funding_entity) "
            "VALUES %s on conflict (archive_id) do nothing;")
        insert_template = (
            "(%(archive_id)s, %(ad_creative_body)s, %(ad_creation_time)s, %(ad_delivery_stop_time)s, %(page_id)s, "
            "%(currency)s, %(ad_creative_link_caption)s, %(ad_creative_link_title)s, %(ad_creative_link_description)s "
            "%(ad_snapshot_url)s, %(funding_entity)s)")
        new_ad_list = [x._asdict() for x in new_ads]
        psycopg2.extras.execute_values(
            cursor, ad_insert_query, new_ad_list, template=insert_template, page_size=500)

    def insert_new_impressions(self, new_impressions):
        cursor = self.get_cursor()
        impressions_insert_query = (
            "INSERT INTO impressions(archive_id, ad_status, min_spend, max_spend, "
            "min_impressions, max_impressions, last_active) "
            "VALUES %s on conflict (archive_id) do update set "
            "ad_status = EXCLUDED.ad_status, min_spend = EXCLUDED.min_spend, max_spend = EXCLUDED.max_spend, "
            "min_impressions = EXCLUDED.min_impressions, max_impressions = EXCLUDED.max_impressions, "
            "last_active = EXCLUDED.last_active;")

        insert_template = (
            "(%(archive_id)s, %(ad_status)s , %(spend__lower_bound)s, %(spend__upper_bound)s , "
            "%(impressions__lower_bound)s , %(impressions_upper_bound)s %(last_active)s)")
        new_impressions_list = [impression._asdict() for impression in new_impressions]

        psycopg2.extras.execute_values(
            cursor, impressions_insert_query, new_impressions_list, template=insert_template, page_size=250)

    def insert_new_impression_demos(self, new_ad_demo_impressions, crawl_date):
        cursor = self.get_cursor()
        demo_impressions_list = [impression._asdict() for impression in new_ad_demo_impressions]
        impression_demo_insert_query = (
            "INSERT INTO demo_impressions(archive_id, age_group, gender, spend_percentage) "
            "VALUES (%s) on conflict on constraint unique_demos_per_ad do update set "
            "percentage = EXCLUDED.percentage;")
        insert_template = '(%(archive_id)s, %(age_range)s, %(gender)s, %(spend_percentage)s)'

        psycopg2.extras.execute_values(
            cursor, impression_demo_insert_query, demo_impressions_list, template=insert_template, page_size=250)
        cursor = self.get_cursor()

        impression_demo_result_insert_query = "INSERT INTO demo_impressions_results(archive_id, age_group, gender, min_impressions, min_spend, max_impressions, max_spend) VALUES %s \
                on conflict on constraint unique_demos_per_ad do update set \
                min_impressions = EXCLUDED.min_impressions, min_spend = EXCLUDED.min_spend, max_impressions = EXCLUDED.max_impressions, max_spend = EXCLUDED.max_spend;"
        insert_template = '(%(archive_id)s, %(age_range)s, %(gender)s, %(min_impressions)s , %(min_spend)s , %(max_impressions)s , %(max_spend)s, %(crawl_date)s)'
        psycopg2.extras.execute_values(
            cursor, impression_demo_result_insert_query, demo_impressions_list, template=insert_template, page_size=250)


    def insert_new_impression_regions(self, new_ad_region_impressions, existing_regions, crawl_date):
        cursor = self.get_cursor()
        impression_region_insert_query = "INSERT INTO region_impressions(ad_archive_id, region_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES %s \
                        on conflict on constraint region_impressions_unique_ad_archive_id do update set crawl_date = EXCLUDED.crawl_date, \
                        min_impressions = EXCLUDED.min_impressions, min_spend = EXCLUDED.min_spend, max_impressions = EXCLUDED.max_impressions, max_spend = EXCLUDED.max_spend;"
        insert_template = "(%(archive_id)s, %(region_id)s, %(min_impressions)s, %(min_spend)s, %(max_impressions)s, %(max_spend)s, %(crawl_date)s)"
        region_impressions_list = []
        for unused_archive_id, region_impression_dict in new_ad_region_impressions.items():
            for unused_region, impression in region_impression_dict.items():
                impression = impression._asdict()
                impression['crawl_date'] = crawl_date
                impression['region_id'] = existing_regions[impression['name']]
                region_impressions_list.append(impression)
        psycopg2.extras.execute_values(
            cursor, impression_region_insert_query, region_impressions_list, template=insert_template, page_size=250)


    def insert_candidate_data(self, candidate_data):
        cursor = self.get_cursor()
        candidate_insert_query = "INSERT INTO candidate_data(candidate_name, twitter_handle, facebook_page_name, party, riding) VALUES %s;"
        insert_template = '("%(candidate_name)s", "%(twitter_handle)s" , "%(facebook_page_name)s" , "%(party)s" , "%(riding)s")'
        records = [insert_template % candidate_row for candidate_row in candidate_data]
        # print(candidate_insert_query % ','.join(records))
        with open('/home/divam/projects/insert.sql','w') as f:
            f.write(candidate_insert_query % ',\n'.join(records))
        # psycopg2.extras.execute_values(
        #     cursor, candidate_insert_query, candidate_data, template=insert_template, page_size=250)
