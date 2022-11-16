
from models import Accounts,Posts,PostStatisiticsActual,PostStatisiticsExpected,Media,PostDashboard

class CrowdtangleDAL():
    def __init__(self,db_session,posts_data) -> None:
        self.db_session = db_session
        self.posts_data = posts_data
        self.accounts = []
        self.statistics = []
        self.posts = []
        self.media = []

    def create_accounts_obj(self):
        for post_data in self.posts_data:
            self.accounts.append(Accounts(
                id = post_data.get('account',{}).get('id'),
                account_type = post_data.get('account',{}).get('accountType'),
                handle = post_data.get('account',{}).get('handle'),
                name = post_data.get('account',{}).get('name'),
                page_admin_top_country = post_data.get('account',{}).get('pageAdminTopCountry'),
                platform = post_data.get('account',{}).get('platform'),
                platform_id = post_data.get('account',{}).get('platformId'),
                profile_image = post_data.get('account',{}).get('profileImage'),
                subscriber_count = post_data.get('account',{}).get('subscriberCount'),
                url = post_data.get('account',{}).get('url'),
                verified = post_data.get('account',{}).get('verified')
            ))

    def create_posts_obj(self):
        for post_data in self.posts_data:
            self.posts.append(Posts(
                id = post_data['id'],
                account_id = post_data.get('account',{}).get('id'),
                branded_content_sponsor_account_id = None,
                message = post_data.get('message'),
                title = None,
                platform = post_data.get('platform'),
                platform_id = post_data.get('platformId'),
                post_url = post_data.get('postUrl'),
                subscriber_count = post_data.get('subscriberCount'),
                type = post_data.get('status'),
                video_length_ms = None,
                image_text = post_data.get('imageText'),
                legacy_id = post_data.get('legacyId'),
                caption = None,
                link = post_data.get('link'),
                date = post_data.get('date'),
                description = None,
                score = None,
                live_video_status = None,
                # message_sha256_hash = Column(Text)
                language_code = post_data.get('languageCode')
            ))

    def create_statistics_obj(self):
        for post_data in self.posts_data:
            self.statistics.extend([PostStatisiticsActual(
                post_id = post_data.get('id'),
                # updated = Column(DateTime(True))
                angry_count = post_data.get('statistics',{}).get('actual',{}).get('angryCount'),
                comment_count = post_data.get('statistics',{}).get('actual',{}).get('commentCount'),
                # favorite_count = post_data.get('statistics',{}).get('actual',{}).get('commentCount'),
                haha_count = post_data.get('statistics',{}).get('actual',{}).get('hahaCount'),
                like_count = post_data.get('statistics',{}).get('actual',{}).get('likeCount'),
                love_count = post_data.get('statistics',{}).get('actual',{}).get('loveCount'),
                sad_count = post_data.get('statistics',{}).get('actual',{}).get('sadCount'),
                share_count = post_data.get('statistics',{}).get('actual',{}).get('shareCount'),
                # up_count = post_data.get('statistics',{}).get('actual',{}).get('commentCount'),
                wow_count = post_data.get('statistics',{}).get('actual',{}).get('wowCount'),
                thankful_count = post_data.get('statistics',{}).get('actual',{}).get('thankfulCount'),
                care_count = post_data.get('statistics',{}).get('actual',{}).get('careCount'),
            ),
            PostStatisiticsExpected(
                post_id = post_data['id'],
                # updated = Column(DateTime(True))
                angry_count = post_data.get('statistics',{}).get('expected',{}).get('angryCount'),
                comment_count = post_data.get('statistics',{}).get('expected',{}).get('commentCount'),
                # favorite_count = post_data['statistics')['actual')['commentCount'),
                haha_count = post_data.get('statistics',{}).get('expected',{}).get('hahaCount'),
                like_count = post_data.get('statistics',{}).get('expected',{}).get('likeCount'),
                love_count = post_data.get('statistics',{}).get('expected',{}).get('loveCount'),
                sad_count = post_data.get('statistics',{}).get('expected',{}).get('sadCount'),
                share_count = post_data.get('statistics',{}).get('expected',{}).get('shareCount'),
                # up_count = post_data['statistics']['actual']['commentCount'],
                wow_count = post_data.get('statistics',{}).get('expected',{}).get('wowCount'),
                thankful_count = post_data.get('statistics',{}).get('expected',{}).get('thankfulCount'),
                care_count = post_data.get('statistics',{}).get('expected',{}).get('careCount')
            )])

    def create_media_obj(self):
        for post_data in self.posts_data:
            for media in post_data.get('media',[]):
                self.media.append(Media(
                    post_id = post_data['id'],
                    url_full = media.get('full'),
                    url = media.get('url'),
                    width = media.get('width'),
                    height = media.get('height'),
                    type = media.get('type'),
                    # nyu_sha256_hash = Column(String)
                    # nyu_bucket_path = Column(String)
                    # nyu_sim_hash = Column(String)
                ))

    def create_post_dashboards_obj(self):
        self.post_dashboards = PostDashboard()
        # session.add(c1)

    def push_to_db(self):
        # all_objects = [self.post_dashboards]
        all_objects = self.accounts+self.posts+self.media+self.statistics
        self.db_session.bulk_save_objects(all_objects)
        self.db_session.commit()
