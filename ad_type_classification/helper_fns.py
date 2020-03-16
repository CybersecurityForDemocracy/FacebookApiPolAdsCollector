import re
from urllib import parse
import logging
import pandas

def get_canonical_url(url):
    try:
        parsed_url = parse.urlparse(str(url))
        domain = str(parsed_url.netloc).lower()
        path = str(parsed_url.path).lower()
        if domain.startswith('www.'):
            domain=domain[4:]
        return domain+path
    except Exception:
        print('Could not process url:', url)
        return ''
  
def find_urls(string): 
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$\-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', string)
    return urls

def get_creative_url(row):
    url = ''
    if row.get('ad_creative_link_caption'):
        url = row['ad_creative_link_caption']
    else:
        urls = find_urls(row['ad_creative_body'])
        if urls:
            url = urls[0]
    return get_canonical_url(url)


def get_lookup_table():
    df = pandas.read_csv('ad_url_to_type.csv')
    df['ad_type'] = df['ad_type'].astype('category')
    print(df.info())
    print(df)
    df_as_dicts = df.to_dict(orient='records')
    lookup_table = {}
    for row in df_as_dicts:
        lookup_table[row['normalized_url']]=row['ad_type']
    lookup_table['']='INFORM'
    return lookup_table
