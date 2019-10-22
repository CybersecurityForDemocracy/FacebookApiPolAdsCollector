import requests
import json

headers = {
    'Content-type': 'application/json',
}

def notify_slack(devops_channel_url, message):
    data = json.dumps({'text': message})
    requests.post(devops_channel_url, headers=headers, data=data)
    
    # Curl request: 
    # curl -X POST -H 'Content-type: application/json' --data '{"text":"Hello, World!"}' https://hooks.slack.com/services/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


# if __name__ == '__main__':
#     notify_slack('Test String with `fun\'ny" characters')
