from db_engine_function import get_crowdtangle_db_session
from crowdtangle_dal import CrowdtangleDAL
from utils import load_config

import bz2
import json

config = load_config()
Session = get_crowdtangle_db_session(config['DATABASE'])


def load_raw_crowdtangle_files(path,batch_size):
    with bz2.open(path, "rt") as bzinput:
        posts = []
        for i, line in enumerate(bzinput):
            post = json.loads(line)
            posts.append(post)
            if i%batch_size==0 and i!=0:
                try:
                    write_crowdtangle_post_to_db(post_data=posts)
                except Exception as e:
                    write_posts_to_dead_letter_queue(posts,e)
                posts = []
    print(path,"file processed")
    return

def write_crowdtangle_post_to_db(post_data):
    with Session() as session:
        dal = CrowdtangleDAL(session,post_data)
        dal.upsert_accounts()
        dal.upsert_posts()
        dal.upsert_statistics()
        # dal.upsert_media_data()
        dal.upsert_expanded_links()
        # dal.create_post_dashboards_obj()
        dal.push_to_db()
    

def write_posts_to_dead_letter_queue(post_data,error):
    # For now we will write it to a .json.bz2 file and have error table in the crowdtangle db
    # id, main_file_name or id, error_message, new_file_path
    print(error)
    pass



if __name__ == "__main__":
    load_raw_crowdtangle_files("/Users/akashmishra/csd/mdev/ukr_noUA_noRU-20221027-180750.json.bz2",20)
    