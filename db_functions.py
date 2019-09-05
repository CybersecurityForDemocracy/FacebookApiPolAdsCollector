import psycopg2


class DBInterface():
    def __init__(self, connection):
        self.connection = connection

    def get_cursor(self):
        return self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def existing_ads(self):
        cursor = self.get_cursor()
        existing_ad_query = "select archive_id, is_active from ads"
        cursor.execute(existing_ad_query)
        ad_ids = set()
        active_ads = set()
        for row in cursor:
            ad_ids.add(row['archive_id'])
            if row['is_active'] == True:
                active_ads.add(row['archive_id'])

        return (ad_ids, active_ads)

    def existing_demos(self):
        cursor = self.get_cursor()
        existing_demo_group_query = "select gender, age, id from demo_groups;"
        cursor.execute(existing_demo_group_query)
        existing_demo_groups = {}
        for row in cursor:
            existing_demo_groups[row['age']+row['gender']] = row['id']

        return existing_demo_groups

    def existing_region(self):
        cursor = self.get_cursor()
        existing_regions_query = "select name, id from regions;"
        cursor.execute(existing_regions_query)
        existing_regions = {}
        for row in cursor:
            existing_regions[row['name']] = row['id']

        return existing_regions

    def existing_page(self):
        cursor = self.get_cursor()
        existing_pages_query = "select page_id, page_name from pages;"
        cursor.execute(existing_pages_query)
        existing_pages = set()
        for row in cursor:
            existing_pages.add(row['page_id'])

        return existing_pages

    def existing_sponsors(self):
        cursor = self.get_cursor()
        existing_ad_sponsor_query = "select id, name from ad_sponsors;"
        cursor.execute(existing_ad_sponsor_query)
        existing_ad_sponsors = {}
        for row in cursor:
            existing_ad_sponsors[row['name']] = row['id']

        return existing_ad_sponsors

    def insert_ad_sponsors(self, new_ad_sponsors):
        cursor = self.get_cursor()
        insert_ad_sponsor = "INSERT INTO ad_sponsors(name) VALUES "
        ad_sponsor_count = 0
        for ad_sponsor in new_ad_sponsors:
            insert_ad_sponsor += cursor.mogrify("(%s),",
                                                (ad_sponsor,)).decode('utf-8')
            ad_sponsor_count += 1

            if ad_sponsor_count >= 250:
                insert_ad_sponsor = insert_ad_sponsor[:-1]
                insert_ad_sponsor += ";"
                # print(cursor.mogrify(insert_ad_sponsor))
                cursor.execute(insert_ad_sponsor)
                insert_ad_sponsor = "INSERT INTO ad_sponsors(name) VALUES "
                ad_sponsor_count = 0

        if ad_sponsor_count > 0:
            insert_ad_sponsor = insert_ad_sponsor[:-1]
            insert_ad_sponsor += ";"
            # print(self.mogrify(insert_ad_sponsor))
            cursor.execute(insert_ad_sponsor)

    def insert_pages(self, new_pages):
        cursor = self.get_cursor()
        insert_page = "INSERT INTO pages(page_id, page_name) VALUES "
        page_count = 0
        for page in new_pages:
            insert_page += cursor.mogrify("(%s, %s),",
                                          (page.id, page.name)).decode('utf-8')
            page_count += 1

            if page_count >= 250:
                insert_page = insert_page[:-1]
                insert_page += ";"
                # print(self.mogrify(insert_page))
                cursor.execute(insert_page)
                insert_page = "INSERT INTO pages(page_id, page_name) VALUES "
                page_count = 0

        insert_page = insert_page[:-1]
        insert_page += ";"
        if page_count > 0:
            # print(self.mogrify(insert_page))
            cursor.execute(insert_page)

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

    def insert_new_ads(self, new_ads, country_code, currency, existing_ad_sponsors):
        cursor = self.get_cursor()
        ad_insert_query = "INSERT INTO ads(archive_id, creation_date, start_date, end_date, currency, page_id, snapshot_url, text, ad_sponsor_id, is_active, link_caption, link_description, link_title, country_code) VALUES %s on conflict on constraint unique_ad_archive_id do nothing;"
        insert_template = '(%(archive_id)s, %(creation_date)s, %(start_date)s, %(end_date)s, %(currency)s, %(page_id)s, %(image_url)s, %(text)s, %(ad_sponsor_id)s, %(is_active)s, %(ad_creative_link_caption)s, %(ad_creative_link_description)s, %(ad_creative_link_title)s, %(country_code)s)'
        new_ad_list = []
        for ad in new_ads:
            ad_dict = ad._asdict()
            ad_dict['country_code'] = country_code
            ad_dict['ad_sponsor_id'] = existing_ad_sponsors[ad.sponsor_label]
            ad_dict['currency'] = currency
            new_ad_list.append(ad_dict)

        psycopg2.extras.execute_values(
            cursor, ad_insert_query, new_ad_list, template=insert_template, page_size=250)

    def insert_new_impressions(self, new_impressions, crawl_date):
        cursor = self.get_cursor()
        impressions_insert_query = "INSERT INTO impressions(ad_archive_id, crawl_date, min_impressions, min_spend, max_impressions, max_spend) VALUES \
            on conflict on constraint impressions_unique_ad_archive_id do update set crawl_date = EXCLUDED.crawl_date, \
                    min_impressions = EXCLUDED.min_impressions, min_spend = EXCLUDED.min_spend, max_impressions = EXCLUDED.max_impressions, max_spend = EXCLUDED.max_spend;"

        insert_template = '(%(archive_id)s, %(crawl_date)s , %(min_impressions)s , %(min_spend)s , %(max_impressions)s , %(max_spend)s)'
        new_impressions_list = []
        for impression in new_impressions:
            impression = impression._asdict()
            impression['crawl_date'] = crawl_date
            new_impressions_list.append(impression)

        psycopg2.extras.execute_values(
            cursor, impressions_insert_query, new_impressions_list, template=insert_template, page_size=250)

    def insert_new_impression_demos(self, new_ad_demo_impressions, crawl_date, existing_demo_groups):
        cursor = self.get_cursor()
        impression_demo_insert_query = "INSERT INTO demo_impressions(ad_archive_id, demo_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES %s \
                on conflict on constraint demo_impressions_unique_ad_archive_id do update set crawl_date = EXCLUDED.crawl_date, \
                min_impressions = EXCLUDED.min_impressions, min_spend = EXCLUDED.min_spend, max_impressions = EXCLUDED.max_impressions, max_spend = EXCLUDED.max_spend;"
        insert_template = '(%(archive_id)s, %(demo_id)s, %(min_impressions)s , %(min_spend)s , %(max_impressions)s , %(max_spend)s, %(crawl_date)s)'
        new_impressions_list = []
        for unused_archive_id, impression in new_ad_demo_impressions():
            impression = impression._asdict()
            impression['crawl_date'] = crawl_date
            impression['demo_id'] = existing_demo_groups[impression['gender'] +
                                                         impression['age_range']]
            new_impressions_list.append(impression)

        psycopg2.extras.execute_values(
            cursor, impression_demo_insert_query, new_impressions_list, template=insert_template, page_size=250)

    def insert_new_impression_regions(self, new_ad_region_impressions, existing_regions):
        cursor = self.get_cursor()
        impression_region_insert_query = "INSERT INTO region_impressions(ad_archive_id, region_id, min_impressions, min_spend, max_impressions, max_spend, crawl_date) VALUES %s \
                        on conflict on constraint region_impressions_unique_ad_archive_id do update set crawl_date = EXCLUDED.crawl_date, \
                        min_impressions = EXCLUDED.min_impressions, min_spend = EXCLUDED.min_spend, max_impressions = EXCLUDED.max_impressions, max_spend = EXCLUDED.max_spend;"
        insert_template = "(%(archive_id)s, %(region_id)s, %(min_impressions)s, %(min_spend)s, %(max_impressions)s, %(max_spend)s)"
        region_impressions_list = []
        for unused_archive_id, region_impression_dict in new_ad_region_impressions.values():
            for unused_region, impression in region_impression_dict.items():
                impression = impression._asdict()
                impression['region_id'] = existing_regions[impression.name]
                region_impressions_list.append(impression)
        psycopg2.extras.execute_values(
            cursor, impression_region_insert_query, region_impressions_list, template=insert_template, page_size=250)
