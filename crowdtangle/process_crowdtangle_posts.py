from collections import namedtuple
import datetime

import apache_beam as beam

_ID_KEY = 'id'
_STATISTICS_KEY = 'statistics'
_MEDIA_KEY = 'media'
_EXPANDED_LINKS_KEY = 'expandedLinks'
_BRANDED_CONTENT_SPONSOR = 'brandedContentSponsor'

EncapsulatedPost = namedtuple('EncapsulatedPost',
                              ['post',
                               'account_list',
                               'statistics_actual',
                               'statistics_expected',
                               'expanded_links',
                               'media_list'])
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
                         'live_video_status'
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
    def make_account_record(account, updated):
        return AccountRecord(
            id=account['id'],
            account_type=account.get('accountType'),
            handle=account.get('handle'),
            name=account.get('name'),
            page_admin_top_country=account.get('pageAdminTopCountry'),
            platform=account.get('platform'),
            platform_id=account.get('platformId'),
            profile_image=account.get('profileImage'),
            subscriber_count=account.get('subscriberCount'),
            url=account.get('url'),
            verified=account.get('verified'),
            updated=updated)

    @staticmethod
    def make_statistics_record(statistics, post_id, updated):
        return StatisticsRecord(
            post_id=post_id,
            updated=updated,
            angry_count=statistics.get('angryCount'),
            comment_count=statistics.get('commentCount'),
            favorite_count=statistics.get('favoriteCount'),
            haha_count=statistics.get('hahaCount'),
            like_count=statistics.get('likeCount'),
            love_count=statistics.get('loveCount'),
            sad_count=statistics.get('sadCount'),
            share_count=statistics.get('shareCount'),
            up_count=statistics.get('upCount'),
            wow_count=statistics.get('wowCount'),
            thankful_count=statistics.get('thankfulCount'),
            care_count=statistics.get('careCount'))

    def process(self, item):
        post_id = item[_ID_KEY]
        post_updated = datetime.datetime.fromisoformat(
            item['updated']).replace(tzinfo=datetime.timezone.utc)

        post_record = PostRecord(
            id=post_id,
            account_id=item['account']['id'],
            branded_content_sponsor_account_id=item.get('brandedContentSponsor', {}).get('id'),
            message=item.get('message'),
            title=item.get('title'),
            platform=item.get('platform'),
            platform_id=item.get('platformId'),
            post_url=item.get('postUrl'),
            subscriber_count=item.get('subscriberCount'),
            type=item.get('type'),
            updated=post_updated,
            video_length_ms=item.get('videoLengthMs'),
            image_text=item.get('imageText'),
            legacy_id=item.get('legacyId'),
            caption=item.get('caption'),
            link=item.get('link'),
            date=item.get('date'),
            description=item.get('description'),
            score=item.get('score'),
            live_video_status=item.get('liveVideoStatus'),
            language_code=item.get('languageCode'))

        account_list = [self.make_account_record(item['account'], post_updated)]

        if _BRANDED_CONTENT_SPONSOR in item:
            account_list.append(self.make_account_record(item[_BRANDED_CONTENT_SPONSOR],
                                                         post_updated))

        statistics_actual = None
        statistics_expected = None
        if _STATISTICS_KEY in item:
            statistics = item[_STATISTICS_KEY]
            if 'actual' in statistics:
                statistics_actual = self.make_statistics_record(
                    item[_STATISTICS_KEY]['actual'], post_id=post_id, updated=post_updated)
            if 'expected' in statistics:
                statistics_expected = self.make_statistics_record(
                    item[_STATISTICS_KEY]['expected'], post_id=post_id,
                    updated=post_updated)

        media_records = []
        if _MEDIA_KEY in item:
            media_records = [
                MediaRecord(post_id=post_id, updated=post_updated, url_full=medium.get('full'),
                         url=medium.get('url'), width=medium.get('width'),
                         height=medium.get('height'), type=medium.get('type'))
                for medium in item[_MEDIA_KEY]]

        expanded_links = []
        if _EXPANDED_LINKS_KEY in item:
            expanded_links = [
                ExpandedLinkRecord(post_id=post_id, updated=post_updated,
                                   original=link.get('original'), expanded=link.get('expanded'))
                for link in item[_EXPANDED_LINKS_KEY]]

        yield EncapsulatedPost(post=post_record, account_list=account_list,
                                 statistics_actual=statistics_actual,
                                 statistics_expected=statistics_expected,
                                 expanded_links=expanded_links, media_list=media_records)
