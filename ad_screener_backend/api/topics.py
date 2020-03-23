from flask import Blueprint, current_app, jsonify, request

import config_utils
import db_functions


topics = Blueprint('topics', __name__, url_prefix='/topics')

@topics.route('/')
def list_all_topics():
    db_connection = current_app.config['DATABASE_CONNECTION']
    db_interface = db_functions.DBInterface(db_connection)
    return db_interface.all_topics()

@topics.route('/<int:topic_id>')
def get_topic_top_ad(topic_id):
    db_connection = current_app.config['DATABASE_CONNECTION']
    db_interface = db_functions.DBInterface(db_connection)
    return {r['archive_id']: {'min_spend': str(r['min_spend']), 'max_spend': str(r['max_spend'])} for r in
            db_interface.topic_top_ads_by_spend(topic_id)}

