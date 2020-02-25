import collections
import itertools
import logging

import dhash
import pybktree
import simhash

import db_functions
import snapshot_url_util
from lib.unionfind import unionfind

BIT_DIFFERENCE_THRESHOLD = 3

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
    duplicated_simhashes = db_interface.duplicate_ad_creative_text_simhashes()
    for text_hash in duplicated_simhashes:
        archive_ids = db_interface.archive_ids_with_ad_creative_text_simhash(text_hash)
        simhash_to_snapshot_url[text_hash] = snapshot_url_util.construct_snapshot_urls(access_token,
                                                                                       archive_ids)
    return simhash_to_snapshot_url

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
    for text_hash in duplicate_simhashes:
        simhash_to_id[text_hash] = db_interface.ad_creative_ids_with_text_simhash(text_hash)
    return simhash_to_id

def _ad_creative_body_text_similarity_clusters(db_connection, existing_clusters_union_find):
    """Returns unionfind.UnionFind of clusters of creative IDs with similar ad creative body text."""
    db_interface = db_functions.DBInterface(db_connection)

    # Get all ad creative body simhashes from database.
    raw_creative_id_to_simhash = db_interface.all_ad_creative_text_simhashes()

    # Convert map to str id -> Simhash obj
    creative_id_to_simhash = {}
    simhash_to_creative_ids = collections.defaultdict(set)
    for creative_id in raw_creative_id_to_simhash:
        text_simhash_as_int = int(raw_creative_id_to_simhash[creative_id], 16)
        simhash_to_creative_ids[text_simhash_as_int].add(creative_id)
        creative_id_to_simhash[str(creative_id)] = simhash.Simhash(text_simhash_as_int)

    # Create simhash index
    text_simhash_index = simhash.SimhashIndex(creative_id_to_simhash.items(),
                                              k=BIT_DIFFERENCE_THRESHOLD)

    # Process all archive IDs to get clusters of creative_ids with similar text
    for curr_simhash_as_int in simhash_to_creative_ids:
        found = text_simhash_index.get_near_dups(simhash.Simhash(curr_simhash_as_int))
        # Convert found creative IDs back to ints since SimhashIndex converts returns them as
        # strings regardless of the provided type.
        found = [int(x) for x in found]
        # Connect all combinantions (regardless of order) of found simhashes
        for creative_id_pair in itertools.combinations(found, 2):
            existing_clusters_union_find.union(creative_id_pair[0], creative_id_pair[1])


def _ad_creative_image_similarity_clusters(db_connection, existing_clusters_union_find):
    """Returns unionfind.UnionFind of clusters of creative IDs with similar image simhashes.

    Args:
        db_connection: psycopg2.connection connection to database from which to retrieve ad creative
        data.
    Returns:
        List of sets of ad creative (as int) that are similar.
    """
    db_interface = db_functions.DBInterface(db_connection)

    # Get all ad creative images simhashes from database.
    creative_id_to_simhash = db_interface.all_ad_creative_image_simhashes()

    # Create BKTree with dhash bit difference function as distance_function, used to find similar
    # hashes
    image_simhash_tree = pybktree.BKTree(dhash.get_num_bits_different)

    # Create inverse map of simhash -> set of creative ID(s), and normalize creative_id_to_simhash
    # values to ints.
    simhash_to_creative_ids = collections.defaultdict(set)
    for creative_id in creative_id_to_simhash:
        image_simhash_as_int = int(creative_id_to_simhash[creative_id], 16)
        simhash_to_creative_ids[image_simhash_as_int].add(int(creative_id))
        image_simhash_tree.add(image_simhash_as_int)

    # Process all image sim hashes to get clusters of similar image simhashes
    num_simhash_processed = 0
    for curr_simhash in simhash_to_creative_ids:
        if num_simhash_processed % 10000 == 0:
            logging.info('Processed %d image simhashses.', num_simhash_processed)
        num_simhash_processed += 1
        found = image_simhash_tree.find(curr_simhash, BIT_DIFFERENCE_THRESHOLD)
        # BKTree.find returns tuples of form (bit difference, value)
        for _, found_hash in found:
            # Connect all combinantions (regardless of order) of found simhashes
            for creative_id_pair in itertools.combinations(simhash_to_creative_ids[found_hash], 2):
                existing_clusters_union_find.union(creative_id_pair[0], creative_id_pair[1])



def ad_creative_clusters(db_connection):
    all_clusters_union_find = unionfind.UnionFind()
    logging.info('Starting text clustering')
    text_union_find = _ad_creative_body_text_similarity_clusters(db_connection,
                                                                 all_clusters_union_find)
    components = all_clusters_union_find.components()
    logging.info('Got %d text clusters', len(components))
    logging.info('Starting image cluster. Passing in text clusters.')
    image_union_find = _ad_creative_image_similarity_clusters(db_connection, all_clusters_union_find)
    logging.info('Got %d text image clusters', len(components))
    return all_clusters_union_find

