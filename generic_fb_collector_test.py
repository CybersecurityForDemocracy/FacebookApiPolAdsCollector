from collections import defaultdict

from unittest import TestCase
from unittest.mock import MagicMock
from generic_fb_collector import SearchRunner, PageRecord
from db_functions import DBInterface

class GenericFBCollectorTest(TestCase):

    def setUp(self):
        mock_config = defaultdict(dict)
        mock_config['SEARCH']['COUNTRY_CODE'] = 'CA'
        mock_config['FACEBOOK']['TOKEN'] = ''
        mock_config['SEARCH']['SLEEP_TIME'] = 2
        mock_config['SEARCH']['LIMIT'] = 1
        mock_config['SEARCH']['MAX_REQUESTS'] = 1
        self.search_runner = SearchRunner(
            crawl_date='today', connection=MagicMock(), db=MagicMock(DBInterface), config=mock_config)
        self.search_runner.refresh_state()
        self.result = dict()
        self.result['ad_snapshot_url'] = 'https://testurl.invalid?id=111359'
        self.result['page_id'] = '65535'
        self.result['page_name'] = 'TestPage'
        self.result['ad_snapshot_url'] = 'https://testurl.invalid/snapshot?id=111359'
        self.result['ad_creative_body'] = 'This is a pol ad'
        self.result['funding_entity'] = 'TestFundingEntity'
        self.result['ad_creation_time'] = 'today'
        self.result['ad_delivery_start_time'] = 'today'
        self.result['ad_delivery_stop_time'] = 'today'
        self.result['impressions'] = {'upper_bound': '1', 'lower_bound': '0'}
        self.result['spend'] = {'upper_bound': '3', 'lower_bound': '2'}
        self.result['currency'] = 'CAD'
        self.result['ad_creative_link_caption'] = 'creative link' 
        self.result['ad_creative_link_description'] = 'creative link description'
        self.result['ad_creative_link_title'] = 'creative link title'
        self.result['publisher_platform'] = 'instagram'

    def testAdConstructor(self):

        ad = self.search_runner.get_ad_from_result(self.result)
        self.assertEqual(ad.ad_creation_time, 'today')
        self.assertEqual(ad.ad_creative_body, 'This is a pol ad')
        self.assertEqual(ad.ad_creative_link_caption, 'creative link')
        self.assertEqual(ad.ad_creative_link_description, 'creative link description')
        self.assertEqual(ad.ad_creative_link_title, 'creative link title')
        self.assertEqual(ad.ad_delivery_start_time, 'today')
        self.assertEqual(ad.ad_delivery_stop_time, 'today')
        self.assertEqual(ad.ad_snapshot_url, 'https://testurl.invalid/snapshot?id=111359')
        self.assertEqual(ad.ad_status, 0)
        self.assertEqual(ad.archive_id, 111359)
        self.assertEqual(ad.country, 'CA')
        self.assertEqual(ad.currency, 'CAD')
        self.assertEqual(ad.first_crawl_time, 'today')
        self.assertEqual(ad.funding_entity, 'TestFundingEntity')
        self.assertEqual(ad.impressions__lower_bound, '0')
        self.assertEqual(ad.impressions__upper_bound, '1')
        self.assertEqual(ad.page_id, '65535')
        self.assertEqual(ad.page_name, 'TestPage')
        self.assertEqual(ad.publisher_platform, 'instagram')
        self.assertEqual(ad.spend__lower_bound, '2')
        self.assertEqual(ad.spend__upper_bound, '3')
    
    def testPageProcessing(self):
        
        # Test adding new pages
        ad = self.search_runner.get_ad_from_result(self.result)
        self.search_runner.process_page(ad)
        self.assertEqual(self.search_runner.new_pages, {PageRecord('65535', 'TestPage')})
        self.assertIn(65535, self.search_runner.existing_pages)

        # Pages which exist, shouldn't be added again
        self.search_runner.new_pages={}
        self.search_runner.process_page(ad)
        self.assertEqual(self.search_runner.new_pages, {})
    

if __name__ == '__main__':
    test_class = GenericFBCollectorTest()
    test_class.setUp()
    test_class.testPageProcessing()