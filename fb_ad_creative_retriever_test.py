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
import hashlib
import os
import unittest

from fb_ad_creative_retriever import FetchedAdCreativeData
import fb_ad_creative_retriever
import snapshot_url_util

ACCESS_TOKEN = os.environ['FB_ACCESS_TOKEN']


def make_fetched_ad_creative_data(archive_id, creative_body=None, creative_link_url=None,
                                  creative_link_title=None, creative_link_description=None,
                                  creative_link_caption=None, creative_link_button_text=None,
                                  image_url=None, video_url=None):
    return FetchedAdCreativeData(archive_id, creative_body, creative_link_url, creative_link_title,
                                 creative_link_description, creative_link_caption,
                                 creative_link_button_text, image_url, video_url)


class FacebookAdCreativeRetrieverTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.access_token=ACCESS_TOKEN
        self.retriever = fb_ad_creative_retriever.FacebookAdCreativeRetriever(
            db_connection=None, ad_creative_images_bucket_client=None,
            ad_creative_videos_bucket_client=None, archive_screenshots_bucket_client=None,
            access_token=self.access_token, commit_to_db_every_n_processed=None, slack_url=None)
        self.addTypeEqualityFunc(FetchedAdCreativeData, self.assertFetchedAdCreativeDataEqual)

    def tearDown(self):
        self.retriever.chromedriver.quit()

    def make_snapshot_url(self, archive_id):
        return snapshot_url_util.construct_snapshot_urls(self.access_token, [archive_id])[0]

    def assertAdCreativeListEqual(self, creative_list_a, creative_list_b):
        if len(creative_list_a) != len(creative_list_b):
            self.fail(
                'Ad creative lists are different lengths. {} != {}'.format(
                    len(creative_list_a),  len(creative_list_b)))
        for idx in range(len(creative_list_a)):
            self.assertFetchedAdCreativeDataEqual(creative_list_a[idx], creative_list_b[idx],
                                                  creative_index=idx)



    def assertFetchedAdCreativeDataEqual(self, creative_data_a, creative_data_b, creative_index=None):
        self.assertEqual(creative_data_a.archive_id, creative_data_b.archive_id,
                         msg='Archive ID from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.creative_body, creative_data_b.creative_body,
                         msg='Creative body from creative {}'.format(creative_index))
        if creative_data_a.creative_link_url:
            creative_a_link_url_prefix = creative_data_a.creative_link_url.rsplit('&', maxsplit=1)[0]
        else:
            creative_a_link_url_prefix = None
        if creative_data_b.creative_link_url:
            creative_b_link_url_prefix = creative_data_b.creative_link_url.rsplit('&', maxsplit=1)[0]
        else:
            creative_b_link_url_prefix = None

        self.assertEqual(creative_a_link_url_prefix, creative_b_link_url_prefix,
                         msg='Creative link URL from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.creative_link_title, creative_data_b.creative_link_title,
                         msg='Creative link title from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.creative_link_description,
                         creative_data_b.creative_link_description,
                         msg='Creative link description from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.creative_link_caption,
                         creative_data_b.creative_link_caption,
                         msg='Creative link caption from creative {}'.format(creative_index))
        self.assertEqual(creative_data_a.creative_link_button_text,
                         creative_data_b.creative_link_button_text,
                         msg='Creative link button text from creative {}'.format(creative_index))
        if creative_data_a.image_url:
            self.assertTrue(creative_data_b.image_url)
        else:
            self.assertFalse(creative_data_b.image_url)
        if creative_data_a.video_url:
            self.assertTrue(creative_data_b.video_url)
        else:
            self.assertFalse(creative_data_b.video_url)


    def testSingleCreativeTextOnly(self):
        archive_id = 781238262414047
        retrieved_data = (
            self.retriever.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                archive_id, self.make_snapshot_url(archive_id)))
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)
        creative = retrieved_data.creatives[0]
        expected_creative_body='These Liberal Media types should be wiped from the face of television.'
        self.assertAdCreativeListEqual(
            retrieved_data.creatives,
            [make_fetched_ad_creative_data(archive_id=archive_id,
                                           creative_body=expected_creative_body)])

    def testSingleCreativeImageOnly(self):
        archive_id = 1094114480759124
        retrieved_data = (
            self.retriever.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                archive_id, self.make_snapshot_url(archive_id)))
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)
        self.assertAdCreativeListEqual(
            retrieved_data.creatives,
            [make_fetched_ad_creative_data(archive_id=archive_id, creative_body='', image_url=True)]
        )

    def testSingleCreativeVideoOnly(self):
        archive_id = 2622398471408486
        retrieved_data = (
            self.retriever.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                archive_id, self.make_snapshot_url(archive_id)))
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)
        self.assertAdCreativeListEqual(
            retrieved_data.creatives,
            [make_fetched_ad_creative_data(archive_id=archive_id, creative_body='', image_url=True,
                                          video_url=True)])

    def testSingleCreativeImageAndText(self):
        archive_id = 714921558918790
        retrieved_data = (
            self.retriever.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                archive_id, self.make_snapshot_url(archive_id)))
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)
        expected_creative_body=('Proud Sarkozy Bakery shopper since 1981. Raised my daughters on '
                                'Oatmeal bread and Saturday morning Cheese Crowns. Try one.')
        self.assertAdCreativeListEqual(
            retrieved_data.creatives, [make_fetched_ad_creative_data(archive_id=archive_id,
            creative_body=expected_creative_body, image_url=True)])

    def testSingleCreativeVideoAndText(self):
        archive_id = 333715494500930
        retrieved_data = (
            self.retriever.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                archive_id, self.make_snapshot_url(archive_id)))
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)
        expected_creative_body = (
            'As they say, not all skinfolk is kinfolk. There are plenty of Black people out there '
            'spreading misinformation in support of Donald Trump, either because they\'ve been '
            'misled themselves, because they think it adds to their "woke" factor, or both. Don\'t '
            'be fooled by the opposite of what you know to be true about this president just '
            'because someone who looks like you said it.\n\n'
            'We can all fight against this misinformation by calling it out when we hear it and by '
            'voting EARLY. If you qualify, please make sure you are able to mail in your absentee '
            'ballot with time to spare. Register to vote and request an absentee ballot at\n\n'
            '#BlackChurchPAC #BlackChurchVote #OrganizeTheChurch #Vote #Vote2020')
        self.assertAdCreativeListEqual(
            retrieved_data.creatives,
            [make_fetched_ad_creative_data(archive_id=archive_id,
                                           creative_body=expected_creative_body, video_url=True)])

    def testSingleCreativeImageTextAndLink(self):
        archive_id = 2225062044275658
        retrieved_data = (
            self.retriever.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                archive_id, self.make_snapshot_url(archive_id)))
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)
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
        self.assertAdCreativeListEqual(
            retrieved_data.creatives,
            [make_fetched_ad_creative_data(
                archive_id=archive_id,
                creative_body=expected_creative_body,
                creative_link_url=expected_creative_link_url,
                creative_link_description='RSVP: Join Bernie and AOC in Des Moines',
                creative_link_caption='RSVP now.',
                creative_link_button_text='Sign Up', 
                image_url=True, video_url=False)])

    def testSingleCreativeVideoTextAndLink(self):
        archive_id = 515029052382226
        retrieved_data = (
            self.retriever.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                archive_id, self.make_snapshot_url(archive_id)))
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)
        creative = retrieved_data.creatives[0]
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
        self.assertAdCreativeListEqual(
            retrieved_data.creatives,
            [make_fetched_ad_creative_data(
                    archive_id=archive_id,
                creative_body=expected_creative_body,
                creative_link_url=expected_creative_link_url,
                creative_link_title='SECURE.ACTBLUE.COM',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption=expected_creative_link_caption,
                creative_link_button_text='Donate Now', image_url=True, video_url=True)])

    def testSingleCreativeImageTextAndLink(self):
        archive_id = 3142605315791574
        retrieved_data = (
            self.retriever.get_creative_data_list_via_chromedriver_with_retry_on_driver_error(
                archive_id, self.make_snapshot_url(archive_id)))
        self.assertIsNotNone(retrieved_data.screenshot_binary_data)

        expected_creatives = [
            FetchedAdCreativeData(
                archive_id=3142605315791574, creative_body=('Illegal poaching is on the rise and '
                'puts already endangered animals at an even greater risk. Help protect endangered '
                'species from increased threats during this global crisis and beyond.'),
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Your Gift Helps Protect Endangered Species',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/99130734_3142605395791566_7122455412120485888_n.jpg?_nc_cat=100&_nc_sid=cf96c8&_nc_ohc=gHgKWLKfOr8AX_70BH-&_nc_ht=scontent-lga3-1.xx&oh=84630b2da39b18c2b3bbc3b689630a19&oe=5F991337',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574,
                creative_body='Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Help protect animals at risk.',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100325272_3142605425791563_8069878648762531840_n.jpg?_nc_cat=106&_nc_sid=cf96c8&_nc_ohc=-54BVx0lHp4AX_FMQov&_nc_ht=scontent-lga3-1.xx&oh=7b958b642031ed3b541467acf26fd00a&oe=5F9780F4',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574,
                creative_body='While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Time is running out. Act today.',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100047467_3142605439124895_1592358661135532032_n.jpg?_nc_cat=104&_nc_sid=cf96c8&_nc_ohc=eTuUqriuYJkAX8b1tIj&_nc_ht=scontent-lga3-1.xx&oh=e88e75cccab40dbf62da95877a577406&oe=5F9AAF8A',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574,
                creative_body='Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Help save wild animals and the places they call home.',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100418596_3142605382458234_7188989875784253440_n.jpg?_nc_cat=111&_nc_sid=cf96c8&_nc_ohc=c5w9uLza02QAX9QQYo8&_nc_ht=scontent-lga3-1.xx&oh=7be755c5c0e1b0692f41185903415c32&oe=5F9ADDB4',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574, creative_body='Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Your Gift Helps Protect Endangered Species',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100324487_3142605405791565_7160982385457102848_n.jpg?_nc_cat=108&_nc_sid=cf96c8&_nc_ohc=gTL025aPoQ4AX8_haIb&_nc_ht=scontent-lga3-1.xx&oh=ae3af2e78b26cd46bd546a4f7d090a19&oe=5F999CCB',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574, creative_body='While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Help protect animals at risk.',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100371925_3142605412458231_2837179701233975296_n.jpg?_nc_cat=108&_nc_sid=cf96c8&_nc_ohc=z3c-8xNLsgIAX-yuMFF&_nc_ht=scontent-lga3-1.xx&oh=a9ef1b6ca8c795361e9f72172db5d62c&oe=5F9A18A8',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574,
                creative_body='Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Time is running out. Act today.',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100047194_3142605435791562_2797997775449489408_n.jpg?_nc_cat=103&_nc_sid=cf96c8&_nc_ohc=xIFUBhcoXSMAX9i0Hid&_nc_oc=AQmbd7iew-meNU396yfeeuvRvsz1WACTeTB38lygN7-nFehYq2t6Qfj-P3f_R4fUhcw&_nc_ht=scontent-lga3-1.xx&oh=ba6163e218f0b41486de31719c68e915&oe=5F97E63A',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574,
                creative_body='Poachers have used stay-at-home orders as an invitation to hunt already endangered species. Time is running out for these beautiful animals and your gift can help us protect them.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Help save wild animals and the places they call home.',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100044634_3142605445791561_4645630927379103744_n.jpg?_nc_cat=109&_nc_sid=cf96c8&_nc_ohc=1NZ_6EaeSy8AX9JD862&_nc_ht=scontent-lga3-1.xx&oh=42c922fa555be7393c76acc9b3a2321b&oe=5F994CDB',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574,
                creative_body='While the world battles a global crisis, we are fighting increased threats against already endangered animals. Help us stop illegal wildlife crime and protect the future for these animals.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Your Gift Helps Protect Endangered Species',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100380318_3142605429124896_4049428439503994880_n.jpg?_nc_cat=100&_nc_sid=cf96c8&_nc_ohc=yb_CMp-f0OAAX8bbRLd&_nc_ht=scontent-lga3-1.xx&oh=4a4907ab0e156540e789c6570bc14f61&oe=5F977A66',
                video_url=None),
            FetchedAdCreativeData(
                archive_id=3142605315791574, creative_body='Illegal poaching is on the rise and puts already endangered animals at an even greater risk. Help protect endangered species from increased threats during this global crisis and beyond.',
                creative_link_url='https://l.facebook.com/l.php?u=https%3A%2F%2Fsecure.wcs.org%2Fdonate%2Fdonate-and-fight-save-wildlife&h=AT1Q-h8go7WbTP3kuR1VB4jekC9qLnVyi18HdhKIxS5tYv-BPFfzBWodleKsLQKZ2pOid9Ab1TLPIS3F8Wb2-Bto_BXC4mdqUtcRlXnyzCrL_fyplowNED5fyxglOUBS9Q1I2zITSRIjAQ',
                creative_link_title='SECURE.WCS.ORG',
                creative_link_description='NOT AFFILIATED WITH FACEBOOK',
                creative_link_caption='Help protect animals at risk.',
                creative_link_button_text='Donate Now',
                image_url='https://scontent-lga3-1.xx.fbcdn.net/v/t39.16868-6/s600x600/100445522_3142605399124899_6094252318906122240_n.jpg?_nc_cat=101&_nc_sid=cf96c8&_nc_ohc=m54qmvImoSoAX9ptow1&_nc_ht=scontent-lga3-1.xx&oh=d287f2c50921deedb06a99ea7aaf54fc&oe=5F9A75FD',
                video_url=None)]

        self.assertAdCreativeListEqual(
            retrieved_data.creatives,
            expected_creatives)


if __name__ == '__main__':
    unittest.main()
