import bz2
import json
import logging
import pathlib
import sys
import time
from urllib.parse import urlencode

import click
from datetime import datetime

import ratelimiter
import requests


def output_for(tag, directory):
    if not tag and not directory:
        return sys.stdout
    tag = tag or 'fetch'
    directory = directory or '.'
    ts = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    p = pathlib.Path(directory, f'{tag}-{ts}.ndjson.bz2')
    return bz2.open(p, 'wt')

def starting_url(token):
    params = {
        'token': token,
        'timeframe': '168 HOUR',
        'sortBy': 'date',
        'includeHistory': 'true',
        'count': 100,
    }
    url = 'https://api.crowdtangle.com/posts?' + urlencode(params)
    return url

@ratelimiter.RateLimiter(max_calls=6, period=60)
def get_url(url, retry_count=0, delay=0):
    if delay:
        logging.info(f'sleeping {delay}s on retry {retry_count}')
        time.sleep(delay)
    response = requests.get(url)

    while response.status_code == 429:
        logging.warning(f'rate limit hit; sleeping')
        time.sleep(30)
        response = requests.get(url)
    if response.status_code >= 500:
        retry_count += 1
        if retry_count <= 10:
            logging.warning(f'response failed ({response.status_code}); retrying')
            response = get_url(url, retry_count, 2 * delay if delay else 1)
        else:
            logging.error(f'response failed ({response.status_code}); giving up on retry {retry_count}')
    return response


@click.command()
@click.option('--tag')
@click.option('--directory', type=click.Path(exists=True,dir_okay=True, file_okay=False))
@click.argument('token', type=str)
def cli(tag, directory, token):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    total_posts = 0
    with output_for(tag, directory) as out:
        url = starting_url(token)
        logging.info(f'starting run for {url}')
        while url:
            response = get_url(url)
            if not response or not response.ok:
                logging.fatal(f'Unexpected failure ({response.status_code}) fetching {url}: {response.text}')
                exit(1)
            try:
                j = response.json()
                if 'status' in j and j['status'] != 200:
                    logging.fatal(f'Unexpected Crowdtangle failure ({response.status_code}) fetching {url}: {j}')
                    exit(1)
                else:
                    result = j['result']
                    if 'pagination' in result and 'nextPage' in result['pagination']:
                        url = result['pagination']['nextPage']
                    else:
                        url = None
                    if 'posts' in result:
                        for p in result['posts']:
                            json.dump(p, fp=out)
                            out.write("\n")
                            total_posts += 1
                        out.flush()
            except Exception:
                logging.exception(f'Unexpected parsing failure fetching {url}: {response.status_code} {response.text}')
                exit(1)
    logging.info(f'finished run normally with {total_posts} posts')

if __name__ == '__main__':
    cli()