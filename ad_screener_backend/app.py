from flask import Flask
from ad_screener_backend.api import topics

import config_utils

app = Flask(__name__)
app.register_blueprint(topics.topics)
app.debug = True

def load_config(config_path):
    config = config_utils.get_config(config_path)
    app.config['DATABASE_CONNECTION_PARAMS'] = (
            config_utils.get_database_connection_params_from_config(config))
    app.config['DATABASE_CONNECTION'] = config_utils.get_database_connection(
        app.config['DATABASE_CONNECTION_PARAMS'])
    app.config['FACEBOOK_ACCESS_TOKEN'] = config_utils.get_facebook_access_token(config)
    app.config['COUNTRY_CODE'] = config['SEARCH']['COUNTRY_CODE']


load_config('/home/paul/FacebookApiPolAdsCollector/fb_ad_image_retriever.cfg')


@app.route('/')
def index():
    return 'Hello world'
