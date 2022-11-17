
from models import Accounts,Posts,PostStatisiticsActual,PostStatisiticsExpected,PostDashboard,ExpandedLinks,Media
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

class CrowdtangleDAL():
    def __init__(self,db_session,posts_data) -> None:
        self.db_session = db_session
        self.posts_data = posts_data
        self.posts_data = self.dedupe_records(posts_data,'id')
    
    def dedupe_records(self,records,key):
        dedupeKey = set()
        deduped_records = []
        for record in records:
            if record.get(key) not in dedupeKey:
                deduped_records.append(record)
                dedupeKey.add(record.get(key))
        return deduped_records


    def upsert_stmt(self,TableObj,records,update_set='all',where=None,pkey='id'):
        stmt = insert(TableObj).values(records)
        if update_set=='all':
            update_set = stmt.excluded
        if where is not None:
            where = text(where) #"posts.updated<EXCLUDED.updated"
        stmt = stmt.on_conflict_do_update(
            index_elements=[pkey],
            set_=update_set,
            where=where
        )
        self.db_session.execute(stmt)

    def upsert_accounts(self):
        account_records = []
        for post_data in self.posts_data:
            account_records.append({
                "id":post_data.get('account',{}).get('id'),
                "account_type": post_data.get('account',{}).get('accountType'),
                "handle" :post_data.get('account',{}).get('handle'),
                "name" :post_data.get('account',{}).get('name'),
                "page_admin_top_country" :post_data.get('account',{}).get('pageAdminTopCountry'),
                "platform" :post_data.get('account',{}).get('platform'),
                "platform_id" :post_data.get('account',{}).get('platformId'),
                "profile_image" :post_data.get('account',{}).get('profileImage'),
                "subscriber_count" :post_data.get('account',{}).get('subscriberCount'),
                "url" :post_data.get('account',{}).get('url'),
                "verified" :post_data.get('account',{}).get('verified'),
                "updated":post_data.get('updated')
            })
        account_records = self.dedupe_records(account_records,'id')
        self.upsert_stmt(Accounts,account_records,'all',"accounts.updated<EXCLUDED.updated",'id')

    def upsert_posts(self):
        values = []
        for post_data in self.posts_data:
            values.append({
                "id":post_data['id'],
                "account_id":post_data.get('account',{}).get('id'),
                "branded_content_sponsor_account_id":None,
                "message":post_data.get('message'),
                "title":None,
                "platform":post_data.get('platform'),
                "platform_id":post_data.get('platformId'),
                "post_url":post_data.get('postUrl'),
                "subscriber_count":post_data.get('subscriberCount'),
                "type":post_data.get('type'),
                "updated":post_data.get('updated'),
                "video_length_ms":None,
                "image_text":post_data.get('imageText'),
                "legacy_id":post_data.get('legacyId'),
                "caption":None,
                "link":post_data.get('link'),
                "date":post_data.get('date'),
                "description":None,
                "score":None,
                "live_video_status":None,
                # message_sha256_hash":Column(Text)
                "language_code":post_data.get('languageCode')
            })
        
        # self.upsert_stmt(Posts,values,update_set,"posts.updated<EXCLUDED.updated",'id')

        stmt = insert(Posts).values(values)
        stmt = stmt.on_conflict_do_update(
            # Let's use the constraint name which was visible in the original posts error msg
            index_elements=[Posts.id],

            # The columns that should be updated on conflict
            set_={
                "account_id":stmt.excluded.account_id,
                "branded_content_sponsor_account_id":stmt.excluded.branded_content_sponsor_account_id,
                "message":stmt.excluded.message,
                "title":stmt.excluded.title,
                "platform":stmt.excluded.platform,
                "platform_id":stmt.excluded.platform_id,
                "post_url":stmt.excluded.post_url,
                "subscriber_count":stmt.excluded.subscriber_count,
                "type":stmt.excluded.type,
                "updated":stmt.excluded.updated,
                "video_length_ms":stmt.excluded.video_length_ms,
                "image_text":stmt.excluded.image_text,
                "legacy_id":stmt.excluded.legacy_id,
                "caption":stmt.excluded.caption,
                "link":stmt.excluded.link,
                "date":stmt.excluded.date,
                "description":stmt.excluded.description,
                "score":stmt.excluded.score,
                "live_video_status":stmt.excluded.live_video_status,
                # "message_sha256_hash":stmt.excluded.message_sha256_hash,
                "language_code":stmt.excluded.language_code
            },
            where=Posts.updated<stmt.excluded.updated
        
        )
        self.db_session.execute(stmt)

    def upsert_statistics(self):
        statisticsActual = []
        statisticsExpected = []
        for post_data in self.posts_data:
            statisticsActual.append({
                "post_id":post_data.get('id'),
                "updated":post_data.get('updated'),
                "angry_count":post_data.get('statistics',{}).get('actual',{}).get('angryCount'),
                "comment_count":post_data.get('statistics',{}).get('actual',{}).get('commentCount'),
                "favorite_count":post_data.get('statistics',{}).get('actual',{}).get('favoriteCount'),
                "haha_count":post_data.get('statistics',{}).get('actual',{}).get('hahaCount'),
                "like_count":post_data.get('statistics',{}).get('actual',{}).get('likeCount'),
                "love_count":post_data.get('statistics',{}).get('actual',{}).get('loveCount'),
                "sad_count":post_data.get('statistics',{}).get('actual',{}).get('sadCount'),
                "share_count":post_data.get('statistics',{}).get('actual',{}).get('shareCount'),
                "up_count":post_data.get('statistics',{}).get('actual',{}).get('upCount'),
                "wow_count":post_data.get('statistics',{}).get('actual',{}).get('wowCount'),
                "thankful_count":post_data.get('statistics',{}).get('actual',{}).get('thankfulCount'),
                "care_count":post_data.get('statistics',{}).get('actual',{}).get('careCount'),
            })
            statisticsExpected.append({
                "post_id":post_data.get('id'),
                "updated":post_data.get('updated'),
                "angry_count":post_data.get('statistics',{}).get('expected',{}).get('angryCount'),
                "comment_count":post_data.get('statistics',{}).get('expected',{}).get('commentCount'),
                "favorite_count":post_data.get('statistics',{}).get('expected',{}).get('favoriteCount'),
                "haha_count":post_data.get('statistics',{}).get('expected',{}).get('hahaCount'),
                "like_count":post_data.get('statistics',{}).get('expected',{}).get('likeCount'),
                "love_count":post_data.get('statistics',{}).get('expected',{}).get('loveCount'),
                "sad_count":post_data.get('statistics',{}).get('expected',{}).get('sadCount'),
                "share_count":post_data.get('statistics',{}).get('expected',{}).get('shareCount'),
                "up_count":post_data.get('statistics',{}).get('expected',{}).get('upCount'),
                "wow_count":post_data.get('statistics',{}).get('expected',{}).get('wowCount'),
                "thankful_count":post_data.get('statistics',{}).get('expected',{}).get('thankfulCount'),
                "care_count":post_data.get('statistics',{}).get('expected',{}).get('careCount'),
            })
        TableObjects = [PostStatisiticsActual,PostStatisiticsExpected]
        statistics = [statisticsActual,statisticsExpected]
        for i in range(len(TableObjects)):
            stmt = insert(TableObjects[i]).values(statistics[i])
            stmt = stmt.on_conflict_do_update(
                index_elements=[TableObjects[i].post_id],
                set_=stmt.excluded,
                where=TableObjects[i].updated<stmt.excluded.updated
            )
            self.db_session.execute(stmt)

    # TODO: Figure out how to keep unique entries might use pkey(post_id,url)
    def upsert_media_data(self):
        mediaRecords = []
        for post_data in self.posts_data:
            for media in post_data.get('media',[]):
                mediaRecords.append({
                    "post_id":post_data['id'],
                    "url_full":media.get('full'),
                    "url":media.get('url'),
                    "width":media.get('width'),
                    "height":media.get('height'),
                    "type":media.get('type'),
                    # nyu_sha256_hash = Column(String)
                    # nyu_bucket_path = Column(String)
                    # nyu_sim_hash = Column(String)
                })
        stmt = insert(Media).values(mediaRecords)
        self.db_session.execute(stmt)
    
    # TODO: Figure out how to keep unique entries might use pkey(post_id,original)
    def upsert_expanded_links(self):
        expanded_links = []
        for post_data in self.posts_data:
            for expandedLink in post_data.get('expandedLinks',[]):
                expanded_links.append({
                    "post_id":post_data.get('id'),
                    "expanded":expandedLink.get('expanded'),
                    "original":expandedLink.get('original')
                }) 
        stmt = insert(ExpandedLinks).values(expanded_links)
        self.db_session.execute(stmt)

    def create_post_dashboards_obj(self):
        self.post_dashboards = PostDashboard()
        # session.add(c1)

    def push_to_db(self):
        # all_objects = [self.post_dashboards]
        # all_objects = self.accounts+self.posts+self.media+self.statistics
        # self.db_session.bulk_save_objects(all_objects)
        self.db_session.commit()
