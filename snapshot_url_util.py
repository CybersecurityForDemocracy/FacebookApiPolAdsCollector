"""Basic utility module for working with facebook ad library snapshot URLs."""
import urllib
import logging

FB_AD_SNAPSHOT_BASE_URL = 'https://www.facebook.com/ads/archive/render_ad/'


def construct_snapshot_urls(access_token, archive_ids):
    """Get list of snapshot URLs for the given archive_ids and access_token,

    Args:
        access_token: API access_token to use for URL.
        archive_ids: list of archive IDs make snapshot URLs for.
    Returns:
        list (str) of snapshot URLs with archive ID and access token.
    """
    archive_id_to_snapshot_url = {}
    for archive_id in archive_ids:
        url = '%s?%s' % (FB_AD_SNAPSHOT_BASE_URL,
                         urllib.parse.urlencode({
                             'id': archive_id,
                             'access_token': access_token
                         }))
        logging.debug('Constructed snapshot URL %s', url)
        archive_id_to_snapshot_url[archive_id] = url
    return archive_id_to_snapshot_url
