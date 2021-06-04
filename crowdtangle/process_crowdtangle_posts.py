from collections import namedtuple
import datetime

import apache_beam as beam

_ID_KEY = 'ct_id'
_STATISTICS_KEY = 'statistics'
_MEDIA_KEY = 'media'
_ORIGINAL_LINKS_KEY = 'links'
_EXPANDED_LINKS_KEY = 'expanded_links'
_BRANDED_CONTENT_SPONSOR = 'brandedContentSponsor'

EncapsulatedPost = namedtuple('EncapsulatedPost',
                              ['post',
                               'account_list',
                               'statistics_actual',
                               'statistics_expected',
                               'expanded_links',
                               'media_list',
                               'dashboard_id'])
PostRecord = namedtuple('PostRecord',
                        ['id',
                         'account_id',
                         'branded_content_sponsor_account_id',
                         'message',
                         'title',
                         'platform',
                         'platform_id',
                         'post_url',
                         'subscriber_count',
                         'type',
                         'updated',
                         'video_length_ms',
                         'image_text',
                         'legacy_id',
                         'caption',
                         'link',
                         'date',
                         'description',
                         'score',
                         'live_video_status',
                         'language_code'])
AccountRecord = namedtuple('AccountRecord',
                           ['id',
                            'account_type',
                            'handle',
                            'name',
                            'page_admin_top_country',
                            'platform',
                            'platform_id',
                            'profile_image',
                            'subscriber_count',
                            'url',
                            'verified',
                            'updated'])
StatisticsRecord = namedtuple('StatisticsRecord',
                              ['post_id',
                               'updated',
                               'angry_count',
                               'comment_count',
                               'favorite_count',
                               'haha_count',
                               'like_count',
                               'love_count',
                               'sad_count',
                               'share_count',
                               'up_count',
                               'wow_count',
                               'thankful_count',
                               'care_count'])
ExpandedLinkRecord = namedtuple('ExpandedLinkRecord',
                                ['post_id',
                                 'updated',
                                 'original',
                                 'expanded'])
MediaRecord = namedtuple('MediaRecord',
                         ['post_id',
                          'updated',
                          'url_full',
                          'url',
                          'width',
                          'height',
                          'type'])

class ProcessCrowdTanglePosts(beam.DoFn):
    """Accepts dict representation of crowdtangle post object, and transforms it EncapsulatedPost.
    """
    @staticmethod
    def make_account_record(item, updated):
        return AccountRecord(
            id=item['account_ct_id'],
            account_type=item.get('account_type'),
            handle=item.get('account_handle'),
            name=item.get('account_name'),
            page_admin_top_country=item.get('account_page_admin_top_country'),
            platform=item.get('platform'),
            platform_id=item.get('account_id'),
            profile_image=item.get('account_profile_image'),
            subscriber_count=item.get('account_subscriber_count'),
            url=item.get('accout_url'),
            verified=item.get('accout_verified'),
            updated=updated)

    @staticmethod
    def make_statistics_record(statistics, key_prefix, post_id, updated):
        return StatisticsRecord(
            post_id=post_id,
            updated=updated,
            angry_count=statistics.get(key_prefix + 'angry_count'),
            comment_count=statistics.get(key_prefix + 'comment_count'),
            favorite_count=statistics.get(key_prefix + 'favorite_count'),
            haha_count=statistics.get(key_prefix + 'haha_count'),
            like_count=statistics.get(key_prefix + 'like_count'),
            love_count=statistics.get(key_prefix + 'love_count'),
            sad_count=statistics.get(key_prefix + 'sad_count'),
            share_count=statistics.get(key_prefix + 'share_count'),
            up_count=statistics.get(key_prefix + 'up_count'),
            wow_count=statistics.get(key_prefix + 'wow_count'),
            thankful_count=statistics.get(key_prefix + 'thankful_count'),
            care_count=statistics.get(key_prefix + 'care_count'))

    def process(self, item):
        post_id = item[_ID_KEY]
        post_updated = datetime.datetime.fromisoformat(
            item['updated']).replace(tzinfo=datetime.timezone.utc)

        post_record = PostRecord(
            id=post_id,
            account_id=item['account_ct_id'],
            # TODO(macpd): figure out how new minet CrowdTangleAPIClient handles
            # brandeeContentSponsor
            #  branded_content_sponsor_account_id=item.get('branded_content_sponsor', {}).get('ct_id'),
            branded_content_sponsor_account_id=None,
            message=item.get('message'),
            title=item.get('title'),
            platform=item.get('platform'),
            platform_id=item.get('id'),
            post_url=item.get('post_url'),
            subscriber_count=item.get('subscriber_count'),
            type=item.get('type'),
            updated=post_updated,
            video_length_ms=item.get('video_length_ms'),
            image_text=item.get('image_text'),
            legacy_id=item.get('legacy_id'),
            caption=item.get('caption'),
            link=item.get('link'),
            date=item.get('date'),
            description=item.get('description'),
            score=item.get('score'),
            live_video_status=item.get('live_video_status'),
            language_code=item.get('language_code'))

        account_list = [self.make_account_record(item, post_updated)]

        #  if _BRANDED_CONTENT_SPONSOR in item:
            #  account_list.append(self.make_account_record(item[_BRANDED_CONTENT_SPONSOR],
                                                         #  post_updated))

        statistics_actual = self.make_statistics_record(
            item, key_prefix='actual', post_id=post_id, updated=post_updated)
        statistics_expected = self.make_statistics_record(
            item, key_prefix='expected', post_id=post_id, updated=post_updated)
        #  if _STATISTICS_KEY in item:
            #  statistics = item[_STATISTICS_KEY]
            #  if 'actual' in statistics:
                #  statistics_actual = self.make_statistics_record(
                    #  item[_STATISTICS_KEY]['actual'], post_id=post_id, updated=post_updated)
            #  if 'expected' in statistics:
                #  statistics_expected = self.make_statistics_record(
                    #  item[_STATISTICS_KEY]['expected'], post_id=post_id,
                    #  updated=post_updated)

        media_records = []
        if _MEDIA_KEY in item:
            media_records = [
                MediaRecord(post_id=post_id, updated=post_updated, url_full=medium.get('full'),
                         url=medium.get('url'), width=medium.get('width'),
                         height=medium.get('height'), type=medium.get('type'))
                for medium in item[_MEDIA_KEY]]

        expanded_links = []
        if _ORIGINAL_LINKS_KEY in item:
            zipped_links_iter = itertools.zip_longest(item[_ORIGINAL_LINKS_KEY], item.get(_EXPANDED_LINKS_KEY, []))
            expanded_links = [
                ExpandedLinkRecord(post_id=post_id, updated=post_updated,
                                   original=original_link, expanded=expanded_link)
                for original_link, expanded_link in zipped_links_iter]

        yield EncapsulatedPost(post=post_record, account_list=account_list,
                               statistics_actual=statistics_actual,
                               statistics_expected=statistics_expected,
                               expanded_links=expanded_links, media_list=media_records,
                               dashboard_id=item['dashboard_id'])
