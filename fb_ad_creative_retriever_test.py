"""Unit tests for fb_ad_creative_retriever. This test intentionally uses live data fetch from
facebook's ad archive to confirm that changes to that page's structure does not break collection.
"""

from collections import namedtuple
import logging
import sys
import time
import unittest
import unittest.mock

from fbactiveads.adsnapshots import ad_creative_retriever
from fbactiveads.adsnapshots import browser_context
from fbactiveads.common import config as fbactiveads_config
from fb_ad_creative_retriever import AdCreativeRecord, make_image_hash_file_path, make_video_sha256_hash_file_path

import fb_ad_creative_retriever

# uncomment these lines to get log output to stdout when tests execute
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def make_ad_creative_record(archive_id, ad_creative_body=None, ad_creative_body_language=None,
                            ad_creative_link_url=None, ad_creative_link_title=None,
                            ad_creative_link_description=None, ad_creative_link_caption=None,
                            ad_creative_link_button_text=None, has_image_url=None,
                            image_sim_hash=None, has_video_url=None):
    return AdCreativeRecord(
        archive_id=archive_id, ad_creative_body=ad_creative_body,
        ad_creative_body_language=ad_creative_body_language, text_sha256_hash=None,
        ad_creative_link_url=ad_creative_link_url, ad_creative_link_title=ad_creative_link_title,
        ad_creative_link_description=ad_creative_link_description,
        ad_creative_link_caption=ad_creative_link_caption,
        ad_creative_link_button_text=ad_creative_link_button_text, image_sha256_hash=None,
        image_downloaded_url=has_image_url, image_bucket_path=None, text_sim_hash=None,
        image_sim_hash=image_sim_hash, video_sha256_hash=None, video_downloaded_url=has_video_url,
        video_bucket_path=None)


class FacebookAdCreativeRetrieverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = fbactiveads_config.load_config('fb_ad_creative_retriever_test.cfg')
        cls.creative_retriever_factory = ad_creative_retriever.FacebookAdCreativeRetrieverFactory(config)
        cls.browser_context_factory = browser_context.DockerSeleniumBrowserContextFactory(config)

    def setUp(self):
        self.mock_image_bucket_client = unittest.mock.Mock()
        self.mock_video_bucket_client = unittest.mock.Mock()
        self.mock_screenshot_bucket_client = unittest.mock.Mock()
        self.retriever = fb_ad_creative_retriever.FacebookAdCreativeRetriever(
            db_connection=None, creative_retriever_factory=self.creative_retriever_factory,
            browser_context_factory=self.browser_context_factory,
            ad_creative_images_bucket_client=self.mock_image_bucket_client,
            ad_creative_videos_bucket_client=self.mock_video_bucket_client,
            archive_screenshots_bucket_client=self.mock_screenshot_bucket_client,
            commit_to_db_every_n_processed=None, slack_url=None, slack_user_id_to_include=None)
        self.retriever.reset_creative_retriever()

    def tearDown(self):
        time.sleep(2.0)

    def retrieve_ad(self, archive_id):
        retrieved_data, snapshot_metadata_record = self.retriever.retrieve_ad(archive_id)
        ad_creative_records = self.retriever.process_fetched_ad_creative_data(archive_id, retrieved_data)
        return ad_creative_records, snapshot_metadata_record

    def assertAdCreativeRecordValid(self, ad_creative_record, creative_index=None):
        if ad_creative_record.ad_creative_body:
            self.assertIsNotNone(ad_creative_record.text_sim_hash,
                                 msg='Creative {} has body text, but is missing text_sim_hash'.format(creative_index))
            self.assertIsNotNone(ad_creative_record.text_sha256_hash,
                                 msg='Creative {} has body text, but is missing text_sha256_hash'.format(creative_index))
            self.assertIsNotNone(
                ad_creative_record.ad_creative_body_language,
                msg='Creative {} has body text, but is missing ad_creative_body_language'.format(creative_index))
        if ad_creative_record.image_downloaded_url:
            self.assertIsNotNone(
                ad_creative_record.image_sim_hash,
                msg='Creative {} has image_downloaded_url but is missing image_sim_hash'.format(creative_index))
            self.assertIsNotNone(ad_creative_record.image_sha256_hash,
                msg='Creative {} has image_downloaded_url but is missing image_sha256_hash'.format(creative_index))
            self.assertIsNotNone(ad_creative_record.image_bucket_path,
                msg='Creative {} has image_downloaded_url but is missing image_bucket_path'.format(creative_index))
        if ad_creative_record.video_downloaded_url:
            self.assertIsNotNone(ad_creative_record.video_sha256_hash,
                msg='Creative {} has video_downloaded_url but is missing video_sha256_hash'.format(creative_index))
            self.assertIsNotNone(ad_creative_record.video_bucket_path,
                msg='Creative {} has video_downloaded_url but is missing video_bucket_path'.format(creative_index))

    def assertAdCreativeRecordsValid(self, creative_list):
        for i, creative in enumerate(creative_list):
            self.assertAdCreativeRecordValid(creative, creative_index=i)

    def assertAdCreativeListEqual(self, creative_list_a, creative_list_b):
        if len(creative_list_a) != len(creative_list_b):
            self.fail(
                'Ad creative lists are different lengths. {} != {}'.format(
                    len(creative_list_a),  len(creative_list_b)))
        for idx, (creative_a, creative_b) in enumerate(zip(creative_list_a, creative_list_b)):
            self.assertAdCreativeRecordEqual(creative_a, creative_b, creative_index=idx)

    def assertAdCreativeRecordEqual(self, creative_data_a, creative_data_b, creative_index=None):
        self.assertEqual(creative_data_a.archive_id, creative_data_b.archive_id,
                         msg='Archive ID from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.ad_creative_body, creative_data_b.ad_creative_body,
                         msg='Creative body from creative {}'.format(creative_index))
        if creative_data_a.ad_creative_link_url:
            creative_a_link_url_prefix = creative_data_a.ad_creative_link_url.rsplit('&', maxsplit=1)[0]
        else:
            creative_a_link_url_prefix = None
        if creative_data_b.ad_creative_link_url:
            creative_b_link_url_prefix = creative_data_b.ad_creative_link_url.rsplit('&', maxsplit=1)[0]
        else:
            creative_b_link_url_prefix = None

        self.assertEqual(creative_a_link_url_prefix, creative_b_link_url_prefix,
                         msg='Creative link URL from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.ad_creative_link_title, creative_data_b.ad_creative_link_title,
                         msg='Creative link title from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.ad_creative_link_description,
                         creative_data_b.ad_creative_link_description,
                         msg='Creative link description from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.ad_creative_link_caption,
                         creative_data_b.ad_creative_link_caption,
                         msg='Creative link caption from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.ad_creative_link_button_text,
                         creative_data_b.ad_creative_link_button_text,
                         msg='Creative link button text from creative {}'.format(creative_index))
        if creative_data_a.image_downloaded_url:
            self.assertTrue(creative_data_b.image_downloaded_url,
                         msg='Creative image URL present vs not present from creative {}'.format(creative_index))
        else:
            self.assertFalse(creative_data_b.image_downloaded_url,
                         msg='Creative image URL not present vs present from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.image_sim_hash, creative_data_b.image_sim_hash,
                         msg='Creative image simhash from creative {}'.format(creative_index))
        if creative_data_a.video_downloaded_url:
            self.assertTrue(creative_data_b.video_downloaded_url,
                         msg='Creative video URL present vs not present from creative {}'.format(creative_index))
        else:
            self.assertFalse(creative_data_b.video_downloaded_url,
                         msg='Creative video URL not present vs present from creative {}'.format(creative_index))

    def assertCreativeImagesUploaded(self, ad_creative_records):
        expected_calls = []
        for ad_creative_record in ad_creative_records:
            expected_calls.append(unittest.mock.call.blob(make_image_hash_file_path(ad_creative_record.image_sim_hash)))
            expected_calls.append(unittest.mock.call.blob().upload_from_string(unittest.mock.ANY))
        self.mock_image_bucket_client.assert_has_calls(expected_calls)

    def assertCreativeVideosUploaded(self, ad_creative_records):
        expected_calls = []
        for ad_creative_record in ad_creative_records:
            expected_calls.append(unittest.mock.call.blob(make_video_sha256_hash_file_path(ad_creative_record.video_sha256_hash)))
            expected_calls.append(unittest.mock.call.blob().upload_from_string(unittest.mock.ANY))
        self.mock_video_bucket_client.assert_has_calls(expected_calls)

    def testSingleCreativeTextOnly(self):
        archive_id = 781238262414047
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creative_body='These Liberal Media types should be wiped from the face of television.'
        expected_creatives = [make_ad_creative_record(archive_id=archive_id,
                                                      ad_creative_body=expected_creative_body)]
        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.mock_image_bucket_client.assert_not_called()
        self.mock_video_bucket_client.assert_not_called()

    def testSingleCreativeImageOnly(self):
        archive_id = 1094114480759124
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)

        expected_creatives = [make_ad_creative_record(archive_id=archive_id, has_image_url=True,
                                 image_sim_hash='79d8b83ce22b0f227f008338c0c2ff08')]
        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)
        self.mock_video_bucket_client.assert_not_called()

    def testSingleCreativeVideoOnly(self):
        archive_id = 2622398471408486
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creatives = [make_ad_creative_record(archive_id=archive_id,
                                     ad_creative_link_description='An Important Reminder from IFI',
                                     image_sim_hash='5d5bdb5a1eb6b21c2d0d83dbfe100000',
                                     has_image_url=True, has_video_url=True)]
        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)
        self.assertCreativeVideosUploaded(ad_creative_records)

    def testSingleCreativeImageAndText(self):
        archive_id = 714921558918790
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creative_body=('Proud Sarkozy Bakery shopper since 1981. Raised my daughters on '
                                'Oatmeal bread and Saturday morning Cheese Crowns. Try one.')
        expected_creatives = [
            make_ad_creative_record(archive_id=archive_id, ad_creative_body=expected_creative_body,
                                    has_image_url=True,
                                    image_sim_hash='2f6b3b3a323919339fcdc91c82209173')]
        self.assertAdCreativeListEqual(
            ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)

    def testSingleCreativeVideoAndText(self):
        archive_id = 613656125985578
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creative_body = ('What is Gabrielle Union afraid of? Tune in to the '
                                  '#CarlosWatsonShow to hear more: https://youtu.be/6V8Rf28afoc')
        self.assertAdCreativeListEqual(
            ad_creative_records,
            [make_ad_creative_record(archive_id=archive_id, ad_creative_body=expected_creative_body,
                                     has_image_url=True,
                                     image_sim_hash='864a5e948ccd0dd62406ebf5a0ff0900',
                                     has_video_url=True)])
        print(ad_creative_records[0].video_sha256_hash)
        self.assertCreativeImagesUploaded(ad_creative_records)
        self.assertCreativeVideosUploaded(ad_creative_records)

    def testSingleCreativeImageTextAndLink(self):
        archive_id = 2225062044275658
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creative_body = (
            'Join Bernie Sanders and Alexandria Ocasio-Cortez for a climate crisis summit in Des '
            'Moines! This event is free and open to the public. RSVPS are encouraged.\n\n'
            'Transportation will be provided to students from Iowa State University and Grinnell '
            'College on first come, first served reservations.\n\n'
            'WHERE\n'
            'Drake University - Bell Center\n'
            '1421 27th Street\n'
            'Des Moines, IA 50311\n\n'
            'WHEN\n'
            'Saturday, November 9, 12:00 PM\n'
            'Doors open at 10:00 AM')
        expected_creative_link_url = (
             'https://l.facebook.com/l.php?u=https%3A%2F%2Fact.berniesanders.com%2Fevent%2Fevent-be'
             'rnie-sanders-attend%2F38266%3Fsource%3Dads-fb-191103-desmoinesIA-AOCclimatesummit_rsv'
             'p-demint-V2-h1-p1&')
        expected_creatives = [
            make_ad_creative_record(archive_id=archive_id, ad_creative_body=expected_creative_body,
                                    ad_creative_link_url=expected_creative_link_url,
                                    ad_creative_link_description=(
                                        'RSVP: Join Bernie and AOC in Des Moines'),
                                    ad_creative_link_caption='RSVP now.',
                                    ad_creative_link_title='ACT.BERNIESANDERS.COM',
                                    ad_creative_link_button_text='Sign Up', has_image_url=True,
                                    image_sim_hash='abd022c6122435f028d1208c12fe3f0e')]
        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)

    def testSingleCreativeVideoTextAndLink(self):
        archive_id = 515029052382226
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        creative = ad_creative_records[0]
        self.assertEqual(creative.archive_id, archive_id)
        expected_creative_body = (
            'As a nation, we can’t be great if we’re not good.\n\n'
            'Take a stand with Marianne today: '
            'https://secure.actblue.com/donate/marianne-williamson-2020-committee')
        expected_creative_link_url = (
            'https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.actblue.com%2Fdonate%2Fmarianne-'
            'williamson-2020-committee')
        expected_creative_link_caption = (
            'Click here to help Marianne reach the next round of debates.')
        expected_creatives = [
            make_ad_creative_record(archive_id=archive_id, ad_creative_body=expected_creative_body,
                                    ad_creative_link_url=expected_creative_link_url,
                                    ad_creative_link_title='SECURE.ACTBLUE.COM',
                                    ad_creative_link_description='Are We Good?',
                                    ad_creative_link_caption=expected_creative_link_caption,
                                    ad_creative_link_button_text='Donate Now', has_image_url=True,
                                    image_sim_hash='0c1c94b6bab68e9b69ff1f85e2e0fc12',
                                    has_video_url=True)]
        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)
        self.assertCreativeVideosUploaded(ad_creative_records)

    def testMultipleCreativesEachWithImageTextAndLink(self):
        archive_id = 3142605315791574
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)

        expected_creatives = [
            make_ad_creative_record(
                archive_id=3142605315791574, ad_creative_body=('Illegal poaching is on the rise and '
                'puts already endangered animals at an even greater risk. Help protect endangered '
                'species from increased threats during this global crisis and beyond.'),
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Your Gift Helps Protect Endangered Species',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True,
                image_sim_hash='e484262561624949c3803e7cc926c040'),
            make_ad_creative_record(
                archive_id=3142605315791574,
                ad_creative_body='Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Help protect animals at risk.',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True,
                image_sim_hash='b9291947637373738100111e18800002'),
            make_ad_creative_record(
                archive_id=3142605315791574,
                ad_creative_body='While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Time is running out. Act today.',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True, image_sim_hash='cc5cd49c746427c63cfff0f971002e0e'),
            make_ad_creative_record(
                archive_id=3142605315791574,
                ad_creative_body='Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Help save wild animals and the places they call home.',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True, image_sim_hash='d9dade562e8dcdeb27a1805266bfff7f'),
            make_ad_creative_record(
                archive_id=3142605315791574, ad_creative_body='Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Your Gift Helps Protect Endangered Species',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True, image_sim_hash='4842531b3b030b0d3800009c30034207'),
            make_ad_creative_record(
                archive_id=3142605315791574, ad_creative_body='While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Help protect animals at risk.',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True, image_sim_hash='f3d1d8aea5c54c96fef7f3fb02c34d0c'),
            make_ad_creative_record(
                archive_id=3142605315791574,
                ad_creative_body='Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Time is running out. Act today.',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True, image_sim_hash='e0d0a3a763e3c38eefd00000f906861f'),
            make_ad_creative_record(
                archive_id=3142605315791574,
                ad_creative_body='Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Help save wild animals and the places they call home.',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True, image_sim_hash='cf9b1b1b9b8e8786409ac043131ac3df'),
            make_ad_creative_record(
                archive_id=3142605315791574,
                ad_creative_body='While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Your Gift Helps Protect Endangered Species',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True, image_sim_hash='f4c8aa2cb4e1f3c3f4c580483c812013'),
            make_ad_creative_record(
                archive_id=3142605315791574, ad_creative_body='Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                ad_creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                ad_creative_link_title='SECURE.WCS.ORG',
                ad_creative_link_description='Help protect animals at risk.',
                ad_creative_link_button_text='Donate Now',
                has_image_url=True, image_sim_hash='2f0f4d4d4d2b2b33b27e5508c9b37f6f')
        ]

        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)

    def testCarouselStyleAdTextAndImage(self):
        archive_id = 2853013434745967
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creative_body = ('Proud to join our firefighters and friends at the dedication of '
                                  'this new truck for the Fort Hill fire station.')
        expected_creatives = [
            make_ad_creative_record(archive_id=archive_id,
                                    ad_creative_body=expected_creative_body,
                                    has_image_url=True,
                                    image_sim_hash='32b93534dc666757008b0220900000c0'),
            make_ad_creative_record(archive_id=archive_id,
                                    ad_creative_body=expected_creative_body,
                                    has_image_url=True,
                                    image_sim_hash='8d8d8dc5667737667f101980c9463749'),
            make_ad_creative_record(archive_id=archive_id,
                                    ad_creative_body=expected_creative_body,
                                    has_image_url=True,
                                    image_sim_hash='72696c6deb6e5f2707e37804a7eed804')
        ]
        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)

    def testCarouselStyleAdTextImageAndButtonNoLink(self):
        archive_id = 289864105344773
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creative_body = ('Giving is always better than receiving. Thank you to all the '
                                  'essential workers. We appreciate you!! A special thanks to '
                                  'Royalton Police Department for allowing us to show our support.')

        expected_creatives = [
            make_ad_creative_record(archive_id=archive_id,
                                    ad_creative_body=expected_creative_body,
                                    has_image_url=True,
                                    ad_creative_link_description='Wes Sherrod - SCC',
                                    ad_creative_link_caption='Auto Body Shop',
                                    ad_creative_link_button_text='Send Message',
                                    image_sim_hash='e41c14529c607303c0000056bf0062ff'),
            make_ad_creative_record(archive_id=archive_id,
                                    ad_creative_body=expected_creative_body,
                                    has_image_url=True,
                                    ad_creative_link_description='Wes Sherrod - SCC',
                                    ad_creative_link_caption='Auto Body Shop',
                                    ad_creative_link_button_text='Send Message',
                                    image_sim_hash='c6f2921052c42139000080217f8000ff'),
            make_ad_creative_record(archive_id=archive_id,
                                    ad_creative_body=expected_creative_body,
                                    has_image_url=True,
                                    ad_creative_link_description='Wes Sherrod - SCC',
                                    ad_creative_link_caption='Auto Body Shop',
                                    ad_creative_link_button_text='Send Message',
                                    image_sim_hash='90b08c8cb4e8eadc80f780087e7fe098')
        ]
        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)

    def testCarouselStyleAdTextVideoAndButtonOnEachCarouselItem(self):
        archive_id = 238370824248079
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creative_body = (
            'Looking for a place to livestream and earn money at the same time? Look no further!')

        expected_creatives = [
            make_ad_creative_record(
                archive_id=archive_id, ad_creative_body=expected_creative_body,
                has_image_url=True,
                has_video_url=True,
                ad_creative_link_url='https://l.facebook.com/l.php?u=http%3A%2F%2Fplay.google.com%2Fstore%2Fapps%2Fdetails%3Fid%3Dcom.asiainno.uplive',
                ad_creative_link_description=None,
                ad_creative_link_title=None,
                ad_creative_link_caption=None,
                ad_creative_link_button_text='Install Now',
                image_sim_hash='9cb672736386862318bc0cf7f3000402'),
            make_ad_creative_record(
                archive_id=archive_id, ad_creative_body=expected_creative_body,
                has_image_url=True,
                has_video_url=True,
                ad_creative_link_url='https://l.facebook.com/l.php?u=http%3A%2F%2Fplay.google.com%2Fstore%2Fapps%2Fdetails%3Fid%3Dcom.asiainno.uplive',
                ad_creative_link_description=None,
                ad_creative_link_title=None,
                ad_creative_link_caption=None,
                ad_creative_link_button_text='Install Now',
                image_sim_hash='70e0b0f1b4b0f874fffc786b8f028300'),
            make_ad_creative_record(
                archive_id=archive_id, ad_creative_body=expected_creative_body,
                has_image_url=True,
                has_video_url=True,
                ad_creative_link_url='https://l.facebook.com/l.php?u=http%3A%2F%2Fplay.google.com%2Fstore%2Fapps%2Fdetails%3Fid%3Dcom.asiainno.uplive',
                ad_creative_link_description=None,
                ad_creative_link_title=None,
                ad_creative_link_caption=None,
                ad_creative_link_button_text='Install Now',
                image_sim_hash='3286c7c3c393110ffff10c9f00801308')
        ]
        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)
        self.assertCreativeVideosUploaded(ad_creative_records)

    def testCarouselStyleAdTextImageAndLinkNoButton(self):
        archive_id = 842440562832839
        ad_creative_records, snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertAdCreativeRecordsValid(ad_creative_records)
        expected_creative_body = (
             'Thank you to all our supporters! We are working hard to get all our students on-line '
             'so they can do their homework and get tutoring remotely. Please donate and/or share, '
             'if you can, to get the word out. Stay safe and well!')
        expected_creative_link_title = (
            'I\'M RAISING MONEY FOR SEAL INC. DUE TO THE COVID 19 PANDEMIC OUR STUDENTS ARE HAVING '
            'TO LEARN REMOTELY. THE ONES THAT HAVE TECHNOLOGY AT HOME ARE DOING A FABULOUS JOB '
            'WITH THE VIRTUAL LEARNING. UNFORTUNATELY, MANY OF THEM DO NOT HAVE CHROME BOOKS, WIFI '
            'OR GOOD READING BOOKS AT HOME. YOUR CONTRIBUTION WILL MAKE A HUGE IMPACT, AND WE WILL '
            'BE ABLE TO TUTOR AND MENTOR MORE STUDENTS REMOTELY DURING THIS TOUGH, STAY AT HOME '
            'ORDER. WHETHER YOU DONATE $5 OR $500, EVERY LITTLE BIT HELPS! THANK YOU FOR YOUR '
            'SUPPORT. STAY SAFE AND WELL. SEAL INC. MISSION STEEMENT- SEA LITERACY UPLIFTS THE '
            'LIVES OF REFUGEE STUDENTS IN GREATER MILWAUKEE BY EMPOWERING THEM TO BECOME ENGAGED '
            'MEMBERS OF THIS DIVERSE, DYNAMIC COMMUNITY. OUR GOAL IS TO FOSTER SOCIAL AND '
            'EDUCATIONAL DEVELOPMENT, THUS CREATING COMMUNITY LEADERS AND INSPIRING FUTURE '
            'GENERATIONS. WE ARE ALSO COMMITTED TO PROVIDING THE PEOPLE OF MILWAUKEE AND THE '
            'SURROUNDING SUBURBS THE OPPORTUNITY TO REACH OUT TO THEIR NEIGHBORS AND STRENGTHEN '
            'THE HARMONY IN OUR CITY!')

        expected_creatives = [
                make_ad_creative_record(
                    archive_id=archive_id,
                    ad_creative_body=expected_creative_body,
                    has_image_url=True,
                    ad_creative_link_title=expected_creative_link_title,
                    ad_creative_link_description='Technology and Book fundraiser',
                    image_sim_hash='a42d51513c7e54f0e43c7287f3681cd7'),
                make_ad_creative_record(
                    archive_id=archive_id,
                    ad_creative_body=expected_creative_body,
                    has_image_url=True,
                    ad_creative_link_title=expected_creative_link_title,
                    ad_creative_link_description='Technology and Book fundraiser',
                    image_sim_hash='2438b8b486a6a5930c7ffbb170a0ccef'),
        ]

        self.assertAdCreativeListEqual(ad_creative_records, expected_creatives)
        self.assertCreativeImagesUploaded(ad_creative_records)


if __name__ == '__main__':
    unittest.main()
