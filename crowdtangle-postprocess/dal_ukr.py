
from models import Accounts,Posts,PostStatisiticsActual,PostStatisiticsExpected,Media,PostDashboard

class CrowdtangleDAL():
    def __init__(self,db_session,post_data) -> None:
        self.db_session = db_session
        self.post_data = post_data

    def create_accounts_obj(self):
        self.account = Accounts()

    def create_posts_obj(self):
        self.post = Posts()

    def create_statistics_obj(self,type):
        if type=='actual':
            self.statisticsActual = PostStatisiticsActual()
        elif type=='expected':
            self.statisticsExpected = PostStatisiticsExpected()

    def create_media_obj(self):
        self.media = Media()

    def create_post_dashboards_obj(self):
        self.post_dashboards = PostDashboard()
        # session.add(c1)

    def push_to_db(self):
        self.db_session.add_all([self.account, self.post, self.statisticsActual,self.statisticsExpected,self.media,self.post_dashboards])
        self.db_session.commit()
