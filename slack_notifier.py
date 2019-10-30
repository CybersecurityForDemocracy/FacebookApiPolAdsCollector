import requests
import json
import logging

headers = {
    'Content-type': 'application/json',
}

def notify_slack(devops_channel_url, message):
    logging.info('Slack notification: %s', message)
    if devops_channel_url:
        data = json.dumps({'text': message})
        requests.post(devops_channel_url, headers=headers, data=data)
        return
    else:
        logging.warning('No Slack URL provided, logging locally only.')
    # Curl request: 
    # curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}' <SLACK URL FROM CONFIG FILE>

# Quick and dirty test
# if __name__ == '__main__':
#     notify_slack('Test String with `fun\'ny" characters')
