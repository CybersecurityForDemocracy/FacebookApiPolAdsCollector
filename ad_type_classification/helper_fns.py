import re
from urllib import parse
import logging
import pandas

URL_RE = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$\-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

def _get_canonical_url(url):
    """Parse string for URL, and strip www. if present.

    Args:
        url: str of URL to parse.
    Returns:
        str of URL (with www. stripped if present) of parsed URL. empty string if parsing failed.
    """
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


def get_normalized_creative_url(row):
    """Get URL from ad creative data. Either link caption or parsed from creative body.

    Args:
        row: dict with keys 'ad_creative_link_caption' and 'ad_creative_body'.
    Returns:
        string: domain (ie secure.actblue.com) of link in ad creative.
    """
    url = ''
    if row.get('ad_creative_link_caption'):
        url = row['ad_creative_link_caption']
    else:
        match = re.search(URL_RE, row['ad_creative_body'])
        if match:
            url = _get_canonical_url(match.group(0))
    return url


def get_canonical_url_to_ad_type_table(ad_url_to_type_csv_path):
    """Returns dict of canonical normalized_url -> ad_type.

    Args:
        str path to CSV of ad url and ad type data.
    Returns:
        dict normalized_url -> ad_type.
    """
    df = pandas.read_csv(ad_url_to_type_csv_path)
    df['ad_type'] = df['ad_type'].astype('category')
    df_as_dicts = df.to_dict(orient='records')
    canonical_url_to_ad_type_table = {}
    for row in df_as_dicts:
        canonical_url_to_ad_type_table[row['normalized_url']] = row['ad_type']
    canonical_url_to_ad_type_table[''] = 'INFORM'
    return canonical_url_to_ad_type_table
