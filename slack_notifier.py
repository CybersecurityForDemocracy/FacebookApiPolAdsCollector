"""Module and/or standalone script to sent messages to a given slack webhook URL"""
import json
import logging
import sys

import requests

HEADERS = {
    'Content-type': 'application/json',
}

def notify_slack(devops_channel_url, message):
    """Sends message to devops_channel_url slack webhook URL.

    Args:
        devops_channel_url: str; slack webhookd URL to send message to.
        message: str; message to send
    """
    logging.info('Slack notification: %s', message)
    if not devops_channel_url:
        logging.warning('No Slack URL provided, logging locally only.')
        return

    data = json.dumps({'text': message})
    requests.post(devops_channel_url, headers=HEADERS, data=data)

# Curl request:
# curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}' <SLACK URL>


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('USAGE', sys.argv[0], ' <slack webhook URL> <message>')
        sys.exit(1)
    notify_slack(sys.argv[1], sys.argv[2])
