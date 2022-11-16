from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, Float, text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class Accounts(Base):
   __tablename__ = 'accounts'
   
   id = Column(Integer, primary_key=True)
   account_type = Column(String)
   handle = Column(String)
   name = Column(String,nullable=False)
   page_admin_top_country = Column(String)
   platform = Column(String)
   platform_id = Column(String)
   profile_image = Column(String)
   subscriber_count = Column(Integer)
   url = Column(String)
   verified = Column(Boolean)
   updated = Column(DateTime(True), server_default=text("now()"))
   last_modified_time = Column(DateTime(True))



class Posts(Base):
   __tablename__ = 'posts'
   
   id = Column(String, primary_key=True)
   account_id = Column(Integer)
   branded_content_sponsor_account_id = Column(Integer)
   message = Column(Text)
   title = Column(String)
   platform = Column(String) #redundant
   platform_id = Column(String) #redundant
   post_url = Column(String) 
   subscriber_count = Column(Integer) #redundant
   type = Column(String)
   updated = Column(DateTime(True))
   video_length_ms = Column(Integer)
   image_text = Column(String)
   legacy_id = Column(Integer)
   caption = Column(String)
   link = Column(String)
   date = Column(DateTime(True))
   description = Column(String)
   score = Column(Float) #redundant
   live_video_status = Column(String)
   last_modified_time = Column(DateTime(True), server_default=text("now()"))
   message_sha256_hash = Column(Text)
   language_code = Column(String)


class Statistics(Base):
    __abstract__ = True

    post_id = Column(String, primary_key=True)
    updated = Column(DateTime(True))
    angry_count = Column(Integer)
    comment_count = Column(Integer)
    favorite_count = Column(Integer)
    haha_count = Column(Integer)
    like_count = Column(Integer)
    love_count = Column(Integer)
    sad_count = Column(Integer)
    share_count = Column(Integer)
    up_count = Column(Integer)
    wow_count = Column(Integer)
    thankful_count = Column(Integer)
    care_count = Column(Integer)
    last_modified_time = Column(DateTime(True), server_default=text("now()"))



class PostStatisiticsActual(Statistics):
   __tablename__ = 'post_statisitics_actual'


class PostStatisiticsExpected(Statistics):
   __tablename__ = 'post_statisitics_expected'

class ExpandedLinks(Base):
    __tablename__ = 'expanded_links'

    id = Column(Integer, primary_key=True)
    post_id = Column(String,ForeignKey("public.posts.id"))
    expanded = Column(String)
    original = Column(String)
    last_modified_time = Column(DateTime(True), server_default=text("now()"))


class Media(Base):
    __tablename__ = 'media'

    id = Column(Integer, primary_key=True)
    post_id = Column(String, ForeignKey("public.posts.id"))
    url_full = Column(String)
    url = Column(String)
    width = Column(Integer)
    height = Column(Integer)
    type = Column(String)
    last_modified_time = Column(DateTime(True), server_default=text("now()"))
    nyu_sha256_hash = Column(String)
    nyu_bucket_path = Column(String)
    nyu_sim_hash = Column(String)

class PostDashboard(Base):
    __tablename__ = 'post_dashboards'

    post_id = Column(String, ForeignKey("public.posts.id"),primary_key=True)
    dashboard_id = Column(Integer,primary_key=True)

# Set Index and Other meta info :TODO