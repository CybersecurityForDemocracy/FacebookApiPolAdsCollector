
from models import Accounts,Posts,PostStatisiticsActual,PostStatisiticsExpected,Media,PostDashboard

class CrowdtangleDAL():
    def __init__(self,db_session,posts_data) -> None:
        self.db_session = db_session
        self.posts_data = posts_data

    def create_accounts_obj(self):
        for post_data in self.posts_data:
            self.accounts.append(Accounts(
                id = post_data['account']['id'],
                account_type = post_data['account']['accountType'],
                handle = post_data['account']['handle'],
                name = post_data['account']['name'],
                page_admin_top_country = post_data['account']['pageAdminTopCountry'],
                platform = post_data['account']['platform'],
                platform_id = post_data['account']['platformId'],
                profile_image = post_data['account']['profileImage'],
                subscriber_count = post_data['account']['subscriberCount'],
                url = post_data['account']['url'],
                verified = post_data['account']['verified']
            ))

    def create_posts_obj(self):
        self.posts = []
        for post_data in self.posts_data:
            self.posts.append(Posts(
                id = post_data['id'],
                account_id = post_data['account']['id'],
                branded_content_sponsor_account_id = None,
                message = post_data['message'],
                title = None,
                platform = post_data['platform'],
                platform_id = post_data['platformId'],
                post_url = post_data['postUrl'],
                subscriber_count = post_data['subscriberCount'],
                type = post_data['status'],
                video_length_ms = None,
                image_text = post_data['imageText'],
                legacy_id = post_data['legacyId'],
                caption = None,
                link = post_data['link'],
                date = post_data['date'],
                description = None,
                score = None,
                live_video_status = None,
                # message_sha256_hash = Column(Text)
                language_code = post_data['languageCode']
            ))

    def create_statistics_obj(self):
        self.statistics = []
        for post_data in self.posts_data:
            self.statistics.append(PostStatisiticsActual(
                post_id = post_data['id'],
                # updated = Column(DateTime(True))
                angry_count = post_data['statistics']['actual']['angryCount'],
                comment_count = post_data['statistics']['actual']['commentCount'],
                # favorite_count = post_data['statistics']['actual']['commentCount'],
                haha_count = post_data['statistics']['actual']['hahaCount'],
                like_count = post_data['statistics']['actual']['likeCount'],
                love_count = post_data['statistics']['actual']['loveCount'],
                sad_count = post_data['statistics']['actual']['sadCount'],
                share_count = post_data['statistics']['actual']['shareCount'],
                # up_count = post_data['statistics']['actual']['commentCount'],
                wow_count = post_data['statistics']['actual']['wowCount'],
                thankful_count = post_data['statistics']['actual']['thankfulCount'],
                care_count = post_data['statistics']['actual']['careCount'],
            ),
            PostStatisiticsExpected(
                post_id = post_data['id'],
                # updated = Column(DateTime(True))
                angry_count = post_data['statistics']['expected']['angryCount'],
                comment_count = post_data['statistics']['expected']['commentCount'],
                # favorite_count = post_data['statistics']['actual']['commentCount'],
                haha_count = post_data['statistics']['expected']['hahaCount'],
                like_count = post_data['statistics']['expected']['likeCount'],
                love_count = post_data['statistics']['expected']['loveCount'],
                sad_count = post_data['statistics']['expected']['sadCount'],
                share_count = post_data['statistics']['expected']['shareCount'],
                # up_count = post_data['statistics']['actual']['commentCount'],
                wow_count = post_data['statistics']['expected']['wowCount'],
                thankful_count = post_data['statistics']['expected']['thankfulCount'],
                care_count = post_data['statistics']['expected']['careCount']
            ))

    def create_media_obj(self):
        self.media = []
        for post_data in self.posts_data:
            self.media.append(Media(
                post_id = post_data['id'],
                url_full = post_data['media']['full'],
                url = post_data['media']['url'],
                width = post_data['media']['width'],
                height = post_data['media']['height'],
                type = post_data['media']['type'],
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
