"""Unit tests for fb_ad_creative_retriever. This test intentionally uses live data fetch from
facebook's ad archive to confirm that changes to that page's structure does not break collection.

Ad type cases tested:
- Single creative
-- Text only (781238262414047)
-- Image only (1094114480759124)
-- Video only (2622398471408486)
-- Image and text (714921558918790)
-- Video and text (333715494500930)
-- Image, text, and link (2225062044275658)
-- Video, text, and link (515029052382226)
- Multiple Creatives
-- Image, text, and link (3142605315791574)
- Carousel Style Ads TODO
-- Only image and/or text (2853013434745967)
-- Image and link (with button) (289864105344773)
-- Image and link (without button) (842440562832839)
"""

from collections import namedtuple
import logging
from hashlib import sha256
import os
import sys
import time
import unittest

#  from fb_ad_creative_retriever import FetchedAdCreativeData
from fbactiveads.adsnapshots.ad_creative_retriever import AdScreenshotAndCreatives, FetchedAdCreativeData, AdCreativeLinkAttributes, AdImage, AdType, ArchiveId
from fbactiveads.adsnapshots import ad_creative_retriever
from fbactiveads.adsnapshots import browser_context
from fbactiveads.common import config as fbactiveads_config
from fbactiveads.common.types import AdData, ArchiveId, get_ad_archive_id, get_ad_archive_id_from_dict
import fb_ad_creative_retriever
from typing import List, Optional, Tuple

# uncomment these lines to get log output to stdout when tests execute
# logger = logging.getLogger()
# logger.addHandler(logging.StreamHandler(sys.stdout))
# logger.setLevel(logging.INFO)

class AdSnapshotHelpers:
    @staticmethod
    def sha256(data: Optional[bytes]) -> Optional[str]:
        if data is None:
            return None
        return sha256(data).hexdigest()

    def assert_ad_screenshot_and_creatives(result: AdScreenshotAndCreatives, assert_ad_type: AdType,
                                           assert_creatives: int, assert_extra_urls: int = 0,
                                           assert_extra_texts: int = 0, assert_extra_images: int = 0,
                                           assert_extra_video_images: int = 0,
                                           assert_has_ad_removed_label: bool = False,
                                           assert_has_ad_data_ad_archive_id: Optional[ArchiveId] = None):
        assert result is not None
        assert result.screenshot_binary_data is not None
        assert len(result.screenshot_binary_data) > 0
        assert result.ad_type is assert_ad_type
        assert result.creatives is not None
        assert len(result.creatives) == assert_creatives
        assert len(result.extra_urls) == assert_extra_urls
        assert len(result.extra_texts) == assert_extra_texts
        assert len(result.extra_images) == assert_extra_images
        assert len(result.extra_video_images) == assert_extra_video_images
        assert result.has_ad_removed_label is assert_has_ad_removed_label
        if assert_has_ad_data_ad_archive_id is None:
            assert result.ad_data is None
        else:
            assert get_ad_archive_id_from_dict(result.ad_data) == assert_has_ad_data_ad_archive_id

    @staticmethod
    def assert_fetched_ad_creative_data(result: FetchedAdCreativeData, assert_body: Optional[str] = None,
                                        assert_has_link_attributes: Optional[bool] = None,
                                        assert_image_sha256: Optional[str] = None,
                                        assert_has_video: bool = False,
                                        assert_footer: Optional[str] = None,
                                        assert_reshared_outer_body: Optional[str] = None):
        assert result is not None
        assert result.body == assert_body
        if assert_has_link_attributes:
            assert result.link_attributes is not None
        else:
            assert result.link_attributes is None
        AdSnapshotHelpers.assert_ad_image(result.image, assert_image_sha256=assert_image_sha256)
        if assert_has_video:
            assert result.video_url is not None
        else:
            assert result.video_url is None
        assert result.footer == assert_footer
        assert result.reshared_outer_body == assert_reshared_outer_body

    @staticmethod
    def assert_fetched_ad_creative_data_equal(result1: FetchedAdCreativeData, result2: FetchedAdCreativeData):
        assert (result1 is not None) == (result2 is not None)
        if result1 is None:
            return
        assert result1.body == result2.body
        assert (result1.link_attributes is not None) == (result2.link_attributes is not None)
        if result1.link_attributes is not None:
            assert len(result1.link_attributes) == len(result2.link_attributes)
            AdSnapshotHelpers.assert_ad_creative_link_attributes_equal(result1.link_attributes, result2.link_attributes)
        AdSnapshotHelpers.assert_ad_image_equal(result1.image, result2.image)
        assert (result1.video_url is not None) == (result2.video_url is not None)
        if result1.video_url is not None:
            assert (len(result1.video_url) > 0) == (len(result2.video_url) > 0)
        assert result1.footer == result2.footer
        assert result1.reshared_outer_body == result2.reshared_outer_body

    @staticmethod
    def assert_ad_creative_link_attributes(result: AdCreativeLinkAttributes, assert_url_contains: Optional[str] = None,
                                           assert_title: Optional[str] = None,
                                           assert_secondary_title: Optional[str] = None,
                                           assert_description: Optional[str] = None,
                                           assert_caption: Optional[str] = None,
                                           assert_secondary_caption: Optional[str] = None,
                                           assert_button: Optional[str] = None):
        assert result is not None
        if assert_url_contains is not None:
            assert result.url is not None
            assert assert_url_contains in result.url
        else:
            assert result.url is None
        assert result.title == assert_title
        # TODO(macpd): use this field in creative retriever
        #  assert result.secondary_title == assert_secondary_title
        assert result.description == assert_description
        assert result.caption == assert_caption
        # TODO(macpd): use this field in creative retriever
        #  assert result.secondary_caption == assert_secondary_caption
        assert result.button == assert_button

    @staticmethod
    def assert_ad_creative_link_attributes_equal(result1: AdCreativeLinkAttributes, result2: AdCreativeLinkAttributes):
        assert (result1 is not None) == (result2 is not None)
        if result1 is None:
            return
        assert (result1.url is not None) == (result2.url is not None)
        if result1.url is not None:
            assert (len(result1.url) > 0) == (len(result2.url) > 0)
        assert result1.title == result2.title
        # TODO(macpd): use this field in creative retriever
        #  assert result1.secondary_title == result2.secondary_title
        assert result1.description == result2.description
        assert result1.caption == result2.caption
        # TODO(macpd): use this field in creative retriever
        #  assert (result1.secondary_caption == result2.secondary_caption
                #  or (' people like this' in result1.secondary_caption
                    #  and ' people like this' in result2.secondary_caption))
        assert result1.button == result2.button

    @staticmethod
    def assert_ad_image(result: AdImage, assert_image_sha256: Optional[str] = None):
        if assert_image_sha256 is not None:
            assert result is not None
            assert result.url is not None
            assert len(result.url) > 0
            assert result.binary_data is not None
            assert len(result.binary_data) > 0
            assert AdSnapshotHelpers.sha256(result.binary_data) == assert_image_sha256
        else:
            assert result is None

    @staticmethod
    def assert_ad_image_equal(result1: AdImage, result2: AdImage):
        assert (result1 is not None) == (result2 is not None)
        if result1 is None:
            return
        assert (result1.url is not None) == (result2.url is not None)
        if result1.url is not None:
            assert (len(result1.url) > 0) == (len(result2.url) > 0)
        assert AdSnapshotHelpers.sha256(result1.binary_data) == AdSnapshotHelpers.sha256(result2.binary_data)

def make_fetched_ad_creative_data(creative_body=None, creative_link_url=None,
                                  creative_link_title=None, creative_link_description=None,
                                  creative_link_caption=None, creative_link_button_text=None,
                                  image_url=None, image_sha256_hash=None, video_url=None):
    if any([creative_link_url, creative_link_title, creative_link_description,
            creative_link_caption, creative_link_button_text]):
        link_attributes = AdCreativeLinkAttributes(url=creative_link_url, title=creative_link_title,
                                                   description=creative_link_description,
                                                   caption=creative_link_caption,
                                                   button=creative_link_button_text,
                                                   secondary_title=None, secondary_caption=None)
    else:
        link_attributes = None
    # TODO(macpd): add image binary_data
    ad_image = AdImage(url=image_url, binary_data=None)
    return FetchedAdCreativeData(body=creative_body, link_attributes=link_attributes,
                                 image=ad_image, video_url=video_url, footer=None,
                                 reshared_outer_body=None)
    #  return AdScreenshotAndCreatives(
    #  return FetchedAdCreativeData(archive_id, creative_body, creative_link_url, creative_link_title,
                                 #  creative_link_description, creative_link_caption,
                                 #  creative_link_button_text, image_url, video_url)


class FacebookAdCreativeRetrieverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = fbactiveads_config.load_config('fb_ad_creative_retriever_test.cfg')
        cls.creative_retriever_factory = ad_creative_retriever.FacebookAdCreativeRetrieverFactory(config)
        cls.browser_context_factory = browser_context.DockerSeleniumBrowserContextFactory(config)

    def setUp(self):
        self.maxDiff = None
        config = fbactiveads_config.load_config('fb_ad_creative_retriever_test.cfg')
        #  self.creative_retriever_factory = ad_creative_retriever.FacebookAdCreativeRetrieverFactory(config)
        #  self.browser_context_factory = browser_context.DockerSeleniumBrowserContextFactory(config)
        self.retriever = fb_ad_creative_retriever.FacebookAdCreativeRetriever(
            db_connection=None, creative_retriever_factory=self.creative_retriever_factory,
            browser_context_factory=self.browser_context_factory,
            ad_creative_images_bucket_client=None,
            ad_creative_videos_bucket_client=None, archive_screenshots_bucket_client=None,
            commit_to_db_every_n_processed=None, slack_url=None)

    def tearDown(self):
        time.sleep(2.0)

    def retrieve_ad(self, archive_id):
        with self.browser_context_factory.web_browser() as browser:
            creative_retriever = self.creative_retriever_factory.build(chrome_driver=browser)
            retrieved_data , snapshot_metadata_record = self.retriever.retrieve_ad(archive_id, creative_retriever)
            return retrieved_data, snapshot_metadata_record

    def assertAdCreativeListEqual(self, creative_list_a, creative_list_b):
        if len(creative_list_a) != len(creative_list_b):
            self.fail(
                'Ad creative lists are different lengths. {} != {}'.format(
                    len(creative_list_a),  len(creative_list_b)))
        for idx in range(len(creative_list_a)):
            #  self.assertFetchedAdCreativeDataEqual(creative_list_a[idx], creative_list_b[idx],
                                                  #  creative_index=idx)
            AdSnapshotHelpers.assert_fetched_ad_creative_data_equal(creative_list_a[idx],
                                                                    creative_list_b[idx])


    def assertFetchedAdCreativeDataEqual(self, creative_data_a, creative_data_b, creative_index=None):
        #  self.assertEqual(creative_data_a.archive_id, creative_data_b.archive_id,
                         #  msg='Archive ID from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.body, creative_data_b.body,
                         msg='Creative body from creative {}'.format(creative_index))
        if creative_data_a.link_attributes is None or creative_data_b.link_attributes is None:
            self.assertIsEqual(creative_data_a.link_attributes, creative_data_b.link_attributes)
        else:
            if creative_data_a.link_attributes and creative_data_a.link_attributes.url:
                creative_a_link_attributes_url_prefix = creative_data_a.link_attributes.url.rsplit('&', maxsplit=1)[0]
            else:
                creative_a_link_attributes_url_prefix = None
            if creative_data_b.link_attributes and creative_data_b.link_attributes.url:
                creative_b_link_attributes_url_prefix = creative_data_b.link_attributes.url.rsplit('&', maxsplit=1)[0]
            else:
                creative_b_link_attributes_url_prefix = None
            self.assertEqual(creative_a_link_attributes_url_prefix, creative_b_link_attributes_url_prefix,
                             msg='Creative link URL from creative {}'.format(creative_index))
        #  if creative_data_a.link_attributes:
            #  #  creative_a_link_url_prefix = creative_data_a.creative_link_url.rsplit('&', maxsplit=1)[0]
            #  #  creative_b_link_url_prefix = creative_data_b.creative_link_url.rsplit('&', maxsplit=1)[0]
            #  self.assertEqual(creative_data_a.link_attributes, creative_data_b.link_attributes,
                         #  msg='Link attributes from creative {}'.format(creative_index)) 
        #  else:
            #  self.assertIsNone(creative_data_b.link_attributes)
        #  if creative_data_a.creative_link_url:
            #  creative_a_link_url_prefix = creative_data_a.creative_link_url.rsplit('&', maxsplit=1)[0]
        #  else:
            #  creative_a_link_url_prefix = None
        #  if creative_data_b.creative_link_url:
            #  creative_b_link_url_prefix = creative_data_b.creative_link_url.rsplit('&', maxsplit=1)[0]
        #  else:
            #  creative_b_link_url_prefix = None

        #  self.assertEqual(creative_a_link_url_prefix, creative_b_link_url_prefix,
                         #  msg='Creative link URL from creative {}'.format(creative_index))
        #  self.assertEqual(creative_data_a.creative_link_title, creative_data_b.creative_link_title,
                         #  msg='Creative link title from creative {}'.format(creative_index))
        #  self.assertEqual(creative_data_a.creative_link_description,
                         #  creative_data_b.creative_link_description,
                         #  msg='Creative link description from creative {}'.format(creative_index))
        #  self.assertEqual(creative_data_a.creative_link_caption,
                         #  creative_data_b.creative_link_caption,
                         #  msg='Creative link caption from creative {}'.format(creative_index))
        #  self.assertEqual(creative_data_a.creative_link_button_text,
                         #  creative_data_b.creative_link_button_text,
                         #  msg='Creative link button text from creative {}'.format(creative_index))
        if creative_data_a.image or creative_data_b.image:
            self.assertEqual(creative_data_b.image.url, creative_data_b.image.url,
                         msg='Creative image from creative {}'.format(creative_index))
        if creative_data_a.video_url:
            self.assertTrue(creative_data_b.video_url,
                         msg='Creative video URL present vs not present from creative {}'.format(creative_index))
        else:
            self.assertFalse(creative_data_b.video_url,
                         msg='Creative video URL not present vs present from creative {}'.format(creative_index))


    def testSingleCreativeTextOnly(self):
        archive_id = 781238262414047
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.SIMPLE,
            assert_creatives=1,
            assert_has_ad_data_ad_archive_id=ArchiveId('781238262414047'),
        )
        AdSnapshotHelpers.assert_fetched_ad_creative_data(
            retrieved_data.creatives[0],
            assert_body='These Liberal Media types should be wiped from the face of television.',
        )

    def testSingleCreativeImageOnly(self):
        archive_id = 1094114480759124
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.SIMPLE,
            assert_creatives=1,
            assert_has_ad_data_ad_archive_id=ArchiveId('1094114480759124'),
        )
        AdSnapshotHelpers.assert_fetched_ad_creative_data(
            retrieved_data.creatives[0],
            assert_image_sha256="f2fa97bdb2ef4396f568879a17f1e1e2ece91129ab7cfa606792449352b8c661",
        )

    def testSingleCreativeVideoOnly(self):
        archive_id = 2622398471408486
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.SIMPLE,
            assert_creatives=1,
            assert_has_ad_data_ad_archive_id=ArchiveId('2622398471408486'),
        )
        AdSnapshotHelpers.assert_fetched_ad_creative_data(
            retrieved_data.creatives[0],
            assert_image_sha256='d1db0d3a4d74d47ae67865da1ea322bd54e9c40fc7c9d68eb1fab98712522008',
            assert_has_video=True,
            assert_has_link_attributes=True
        )

    def testSingleCreativeImageAndText(self):
        archive_id = 714921558918790
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.SIMPLE,
            assert_creatives=1,
            assert_has_ad_data_ad_archive_id=ArchiveId('714921558918790'),
        )
        AdSnapshotHelpers.assert_fetched_ad_creative_data(
            retrieved_data.creatives[0],
            assert_body=('Proud Sarkozy Bakery shopper since 1981. Raised my daughters on Oatmeal '
                         'bread and Saturday morning Cheese Crowns. Try one.'),
            assert_image_sha256='53ce065b01e6b77036afc9ebe3827804d145ac5ef4f3df31c8d6e1f200e7ba83'
        )


    def testSingleCreativeVideoAndText(self):
        archive_id = 333715494500930
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.SIMPLE,
            assert_creatives=1,
            assert_has_ad_data_ad_archive_id=ArchiveId('333715494500930'),
        )
        print('image: ', retrieved_data.creatives[0].image)
        AdSnapshotHelpers.assert_fetched_ad_creative_data(
            retrieved_data.creatives[0],
            assert_body=(
                'As they say, not all skinfolk is kinfolk. There are plenty of Black people out '
                'there spreading misinformation in support of Donald Trump, either because '
                'they\'ve been misled themselves, because they think it adds to their "woke" '
                'factor, or both. Don\'t be fooled by the opposite of what you know to be true '
                'about this president just because someone who looks like you said it.\n\nWe can '
                'all fight against this misinformation by calling it out when we hear it and by '
                'voting EARLY. If you qualify, please make sure you are able to mail in your '
                'absentee ballot with time to spare. Register to vote and request an absentee '
                'ballot at\n\n#BlackChurchPAC #BlackChurchVote #OrganizeTheChurch #Vote #Vote2020'),
            assert_has_video=True
        )

    def testSingleCreativeImageTextAndLink(self):
        archive_id = 2225062044275658
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        expected_creative_link_url = (
             'https%3A%2F%2Fact.berniesanders.com%2Fevent%2Fevent-be'
             'rnie-sanders-attend%2F38266%3Fsource%3Dads-fb-191103-desmoinesIA-AOCclimatesummit_rsv'
             'p-demint-V2-h1-p1&')
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.SIMPLE,
            assert_creatives=1,
            assert_has_ad_data_ad_archive_id=ArchiveId('2225062044275658'),
        )
        AdSnapshotHelpers.assert_fetched_ad_creative_data(
            retrieved_data.creatives[0],
            assert_body=(
                'Join Bernie Sanders and Alexandria Ocasio-Cortez for a climate crisis summit in '
                'Des Moines! This event is free and open to the public. RSVPS are encouraged.\n\n'
                'Transportation will be provided to students from Iowa State University and '
                'Grinnell College on first come, first served reservations.\n\n'
                'WHERE\n'
                'Drake University - Bell Center\n'
                '1421 27th Street\n'
                'Des Moines, IA 50311\n\n'
                'WHEN\n'
                'Saturday, November 9, 12:00 PM\n'
                'Doors open at 10:00 AM'),
            assert_image_sha256='828ede101f0c5c6e3665394988424f1c4e92068cc7fa31aa1c7235b5f7b1c3d6',
            assert_has_link_attributes=True
        )
        AdSnapshotHelpers.assert_ad_creative_link_attributes(
            retrieved_data.creatives[0].link_attributes,
            assert_url_contains='act.berniesanders.com',
            assert_title='ACT.BERNIESANDERS.COM',
            assert_description='RSVP: Join Bernie and AOC in Des Moines',
            assert_caption='RSVP now.',
            assert_button='Sign Up')

    def testSingleCreativeVideoTextAndLink(self):
        archive_id = 515029052382226
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.SIMPLE,
            assert_creatives=1,
            assert_has_ad_data_ad_archive_id=ArchiveId('515029052382226'),
        )
        AdSnapshotHelpers.assert_fetched_ad_creative_data(
            retrieved_data.creatives[0],
            assert_body=(
                'As a nation, we can’t be great if we’re not good.\n\n'
                'Take a stand with Marianne today: '
                'https://secure.actblue.com/donate/marianne-williamson-2020-committee'),
            assert_image_sha256='933dad64c474d21fb724cbe64bf003b05437cd20ab660dfb64de60ac94654046',
            assert_has_link_attributes=True,
            assert_has_video=True
        )
        AdSnapshotHelpers.assert_ad_creative_link_attributes(
            retrieved_data.creatives[0].link_attributes,
            assert_url_contains='secure.actblue.com%2Fdonate%2Fmarianne-williamson-2020-committee',
            assert_title='SECURE.ACTBLUE.COM',
            assert_description='Are We Good?',
            assert_caption='Click here to help Marianne reach the next round of debates.',
            assert_button='Donate Now'
        )

    def testCarouselStyleAdImageTextAndLink(self):
        archive_id = 3142605315791574
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.MULTIPLE_VERSIONS,
            assert_creatives=10,
            assert_has_ad_data_ad_archive_id=ArchiveId('3142605315791574'),
        )
        expected_creatives = [
            {
                'assert_body': ('Illegal poaching is on the rise and '
                'puts already endangered animals at an even greater risk. Help protect endangered '
                'species from increased threats during this global crisis and beyond.'),
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Your Gift Helps Protect Endangered Species',
                'assert_button': 'Donate Now',
                'assert_image_sha256': 'd732d6fcebe7bc0bbdc78d5e6f26bf21ef8e96cd0f831e567011c5be6a41172e'},
            {
                'assert_body': 'Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Help protect animals at risk.',
                'assert_button': 'Donate Now',
                'assert_image_sha256': 'b33e6666d1c954bf647d68e1bd38c8eca8f4c2da82afa52c798184a85d9a1f46'},
            {
                'assert_body': 'While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Time is running out. Act today.',
                'assert_button': 'Donate Now',
                'assert_image_sha256': 'e9a54e71ababb469aff3f6210c9432e5a24e4e6ac8afae61b698a5b0a5709227'},
            {
                'assert_body': 'Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Help save wild animals and the places they call home.',
                'assert_button': 'Donate Now',
                'assert_image_sha256': '1e80376dcd5c48f83083d3c94786d16f29e96ca12da0392492315b6243b4fa55'},
            {
                'assert_body': 'Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Your Gift Helps Protect Endangered Species',
                'assert_button': 'Donate Now',
                'assert_image_sha256': 'ba7e8343352968382881006b3688214578b3e48d0e350072a9971af12f70ac0a'},
            {
                'assert_body': 'While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Help protect animals at risk.',
                'assert_button': 'Donate Now',
                'assert_image_sha256': 'ebcff3818a5ce3738e013a383c5ba8a14418c61d292c4810a5f4bb8a0b09b967'},
            {
                'assert_body': 'Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Time is running out. Act today.',
                'assert_button': 'Donate Now',
                'assert_image_sha256': '0dde3c624f2b2c5c8b2983417dfc8d00c6ea7b1df73482601e03fd35b6de7f6a'},
            {
                'assert_body': 'Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Help save wild animals and the places they call home.',
                'assert_button': 'Donate Now',
                'assert_image_sha256': '480f28af2dcbfafb2500e558acf9666c01d59f62328ed548fb4014073a1a9fd4'},
            {
                'assert_body': 'While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Your Gift Helps Protect Endangered Species',
                'assert_button': 'Donate Now',
                'assert_image_sha256': 'bdb02f2824347a6d257548c1be8bccdec618805266294a68f27a83735583ec77'},
            {
                'assert_body': 'Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                'assert_url': 'https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife',
                'assert_title': 'SECURE.WCS.ORG',
                'assert_description': 'Help protect animals at risk.',
                'assert_button': 'Donate Now',
                'assert_image_sha256': '82c907aa3ee81b55118e116267911581aae76d34635653486445e2dcf628e07a'}]
        for creative, expectation in zip(retrieved_data.creatives, expected_creatives):
            AdSnapshotHelpers.assert_fetched_ad_creative_data(
                creative,
                assert_body=expectation['assert_body'],
                assert_image_sha256=expectation['assert_image_sha256'],
                assert_has_link_attributes=True,
            )
            AdSnapshotHelpers.assert_ad_creative_link_attributes(
                creative.link_attributes,
                assert_url_contains=expectation.get('assert_url'),
                assert_title=expectation.get('assert_title'),
                assert_description=expectation.get('assert_description'),
                assert_caption=expectation.get('assert_caption'),
                assert_button=expectation.get('assert_button')
            )

    def testCarouselStyleAdTextAndImage(self):
        archive_id = 2853013434745967
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.CAROUSEL,
            assert_creatives=3,
            assert_has_ad_data_ad_archive_id=ArchiveId('2853013434745967'),
        )
        for creative, image_sha256 in zip(retrieved_data.creatives,
                                          ['56d64a044a94892c967b2747b50ad180a31ff10471e0eee666bb5fec8e56485a',
                                           '85722f26fa340c9d61009e858430cd67296e60a63d47a48d00935f7f400b764c',
                                           '99dcec79c3a20e6cdb988384d8ca249f001b4a511386ac2da8ed09a647111937']):
            AdSnapshotHelpers.assert_fetched_ad_creative_data(
                creative,
                assert_body=('Proud to join our firefighters and friends at the dedication of '
                             'this new truck for the Fort Hill fire station.'),
                assert_image_sha256=image_sha256,
            )

    def testCarouselStyleAdTextImageAndButtonNoLink(self):
        archive_id = 289864105344773
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.CAROUSEL,
            assert_creatives=3,
            assert_has_ad_data_ad_archive_id=ArchiveId('289864105344773'),
        )
        for creative, image_sha256 in zip(retrieved_data.creatives,
                                          ['6f7cb8ebb16a871f863fae93b7a02b9521305ecd4318a7dbc9dbb1c116ad3b6b',
                                           'b997d7270328115b44f7b10d7aa7a45760eae82b03508886cc9acf65d58e9ffc',
                                           'aaeef469cf36288a9f033f03e6d26fe67c4600910c958ebfbf38612809ec36f9']):
            AdSnapshotHelpers.assert_fetched_ad_creative_data(
                creative,
                assert_body=('Giving is always better than receiving. Thank you to all the '
                             'essential workers. We appreciate you!! A special thanks to '
                             'Royalton Police Department for allowing us to show our support.'),
                assert_image_sha256=image_sha256,
                assert_has_link_attributes=True,
            )
            AdSnapshotHelpers.assert_ad_creative_link_attributes(
                creative.link_attributes,
                assert_description='Wes Sherrod - SCC',
                assert_caption='Auto Body Shop',
                assert_button='Send Message',
            )


    def testCarouselStyleAdTextVideoAndButtonOnEachCarouselItem(self):
        archive_id = 238370824248079
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.CAROUSEL,
            assert_creatives=3,
            assert_has_ad_data_ad_archive_id=ArchiveId('238370824248079'),
        )
        for creative, image_sha256 in zip(retrieved_data.creatives,
                                          ['25241aba52a9a0cc5cbcb0ddd549481c03cc954f97cd3e77293f271c7324f954',
                                           '6a205625ad6d6359ba8160968ba8808724086dfe02c96d7948ebf1405625054e',
                                           '0199d8baaef4cb978d3562df2d21f060381900ac634160a88a7d1bb37688dc30']):
            AdSnapshotHelpers.assert_fetched_ad_creative_data(
                creative,
                assert_body=('Looking for a place to livestream and earn money at the same time? '
                             'Look no further!'),
                assert_image_sha256=image_sha256,
                assert_has_link_attributes=True,
                assert_has_video=True
            )
            AdSnapshotHelpers.assert_ad_creative_link_attributes(
                creative.link_attributes,
                assert_button='Install Now',
                assert_url_contains='http%3A%2F%2Fplay.google.com%2Fstore%2Fapps%2Fdetails%3Fid%3Dcom.asiainno.uplive',
            )

    def testCarouselStyleAdTextImageAndLinkNoButton(self):
        archive_id = 842440562832839
        retrieved_data , snapshot_metadata_record = self.retrieve_ad(archive_id)
        AdSnapshotHelpers.assert_ad_screenshot_and_creatives(
            retrieved_data,
            assert_ad_type=AdType.CAROUSEL,
            assert_creatives=2,
            assert_has_ad_data_ad_archive_id=ArchiveId('842440562832839'),
        )
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
        for creative, image_sha256 in zip(retrieved_data.creatives,
                                          ['c4afb65ca60f6d46a6e53639fd8e6008aeede7a0d49444aeac81084510546289',
                                           '7c4dea465e9034b05f8cab8676b56d5e90d6e0eb49578800342274a8492b5555']):
            AdSnapshotHelpers.assert_fetched_ad_creative_data(
                creative,
                assert_body=(
                    'Thank you to all our supporters! We are working hard to get all our students '
                    'on-line so they can do their homework and get tutoring remotely. Please '
                    'donate and/or share, if you can, to get the word out. Stay safe and well!'),
                assert_image_sha256=image_sha256,
                assert_has_link_attributes=True,
            )
            AdSnapshotHelpers.assert_ad_creative_link_attributes(
                creative.link_attributes,
                assert_title=expected_creative_link_title,
                assert_description='Technology and Book fundraiser',
            )


if __name__ == '__main__':
    unittest.main()
