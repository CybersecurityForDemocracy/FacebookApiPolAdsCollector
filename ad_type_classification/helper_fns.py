import re
from urllib import parse
import logging
import pandas

URL_RE = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$\-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

def get_canonical_url(url):
    try:
        parsed_url = parse.urlparse(str(url))
        domain = str(parsed_url.netloc).lower()
        path = str(parsed_url.path).lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain + path
    except ValueError as err:
        logging.info('%s error while processing url: %s', err, url)
        return ''


def get_creative_url(row):
    url = ''
    if row.get('ad_creative_link_caption'):
        url = row['ad_creative_link_caption']
    else:
        match = re.search(URL_RE, row['ad_creative_body'])
        if match:
            url = get_canonical_url(match.group(0))
    return url


def get_lookup_table(ad_url_to_type_csv_path):
    df = pandas.read_csv(ad_url_to_type_csv_path)
    df['ad_type'] = df['ad_type'].astype('category')
    df_as_dicts = df.to_dict(orient='records')
    lookup_table = {}
    for row in df_as_dicts:
        lookup_table[row['normalized_url']] = row['ad_type']
    lookup_table[''] = 'INFORM'
    return lookup_table
