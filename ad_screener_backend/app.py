from flask import Flask, request
from ad_screener_backend.api import topics
from flask_cors import CORS
import config_utils

import json

app = Flask(__name__)
app.register_blueprint(topics.topics)
app.debug = True


CORS(app, origins=["http://ccs3usr.engineering.nyu.edu:8080"])

def load_config(config_path):
    config = config_utils.get_config(config_path)
    app.config['DATABASE_CONNECTION_PARAMS'] = (
            config_utils.get_database_connection_params_from_config(config))
    app.config['DATABASE_CONNECTION'] = config_utils.get_database_connection(
        app.config['DATABASE_CONNECTION_PARAMS'])
    app.config['FACEBOOK_ACCESS_TOKEN'] = config_utils.get_facebook_access_token(config)


load_config('db.cfg')


@app.route('/')
def index():
    print(request.args)
    return topics.get_topic_top_ad(914)

