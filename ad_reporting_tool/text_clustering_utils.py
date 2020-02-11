import db_functions
import snapshot_url_util

def duplicated_ad_creative_body_simhash_snapshot_urls(access_token, db_connection):
    """Get map simhash -> snapshot urls of ads with that creative body simhash.

    Only returns simhashes that appear 2 or more times in ad creatives data.

    Args:
        access_token: str, API token to use in snapshot URLs
        db_connection: psycopg2 database connection.
    Returns:
        Dict simhash (str) -> list of snapshot URLs where one of the ad creative(s) body text has
        the simhash in the key.
    """
    db_interface = db_functions.DBInterface(db_connection)
    simhash_to_snapshot_url = {}
    duplicate_simhashes = db_interface.duplicate_ad_creative_text_simhashes()
    for simhash in duplicate_simhashes:
        archive_ids = self.archive_ids_with_ad_creative_text_simhash(simhash)
        simhash_to_snapshot_url[simhash] = snapshot_url_util.construct_snapshot_urls(access_token,
                                                                                     archive_ids)
    return simhash_to_id

def all_ad_creative_ids_with_duplicated_simhash(db_connection):
    """Returns map of ad creative body simhash -> ad_creative_id for
    simhashes that appear 2 or more times in database.

    Returns: dict of simhash (str) -> ad_creative_id (str) where all
    ad_creative_body simhash match the simhash key. Dict only hash simhashes
    that appear 2 or more times in database.
    """
    db_interface = db_functions.DBInterface(db_connection)
    duplicate_simhashes = db_interface.duplicate_ad_creative_text_simhashes()
    simhash_to_id = {}
    for simhash in duplicate_simhashes:
        simhash_to_id[simhash] = db_interface.ad_creative_ids_with_text_simhash(simhash)
    return simhash_to_id
