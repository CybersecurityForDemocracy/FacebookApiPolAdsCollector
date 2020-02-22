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

def ad_creative_body_text_similarity_clusters(db_connection):
    """Returns list of clusters of archive IDs with similar ad creative body text."""
    db_interface = db_functions.DBInterface(db_connection)

    # Get all ad creative body simhashes from database.
    raw_archive_id_to_simhash = db_interface.all_ad_creative_text_simhashes()

    # TODO(macpd): use ad_creative_id instead of archive_id
    # Convert map to str id -> Simhash obj
    archive_id_to_simhash = {}
    for a in raw_archive_id_to_simhash:
        archive_id_to_simhash[str(a)] = simhash.Simhash(int(raw_archive_id_to_simhash[a],16))
    # Create simhash index
    text_simhash_index = simhash.SimhashIndex(archive_id_to_simhash.items(), k=BIT_DIFFERENCE_THRESHOLD)

    # Process all archive IDs to get clusters of archive_ids with similar text
    seen_archive_ids = set()
    archive_ids_with_similar_text = []
    for archive_id in archive_id_to_simhash:
        if len(seen_archive_ids) % 10000 == 0:
            logging.info('Processed %d archive IDs.', len(seen_archive_ids))
        if archive_id in seen_archive_ids:
            continue

        seen_archive_ids.add(archive_id)
        similar_archive_ids = text_simhash_index.get_near_dups(archive_id_to_simhash[archive_id])
        # TODO(macpd): double check this set optimization logic
        [seen_archive_ids.add(x) for x in similar_archive_ids]
        archive_ids_with_similar_text.append(similar_archive_ids)

    logging.info('Processed %d archive IDs. got %d clusters', len(seen_archive_ids),
                 len(archive_ids_with_similar_text))

    return archive_ids_with_similar_text

def ad_creative_image_similarity_clusters(db_connection):
    """Returns list of clusters of image simhashs of similar ad creative images.

    Args:
        db_connection: psycopg2.connection connection to database from which to retrieve ad creative
        data.
    Returns:
        List of sets of image simhashes (as int) that are similar.
    """
    db_interface = db_functions.DBInterface(db_connection)

    # Get all ad creative images simhashes from database.
    creative_id_to_simhash = db_interface.all_ad_creative_image_simhashes()

    # Create BKTree with dhash bit difference function as distance_function, used to find similar
    # hashes
    image_simhash_tree = pybktree.BKTree(dhash.get_num_bits_different)

    # Create inverse map of simhash -> set of creative ID(s), and normalize creative_id_to_simhash values
    # to ints.
    simhash_to_creative_ids = collections.defaultdict(set)
    for creative_id in creative_id_to_simhash:
        image_simhash_as_int = int(creative_id_to_simhash[creative_id], 16)
        simhash_to_creative_ids[image_simhash_as_int].add(int(creative_id))
        image_simhash_tree.add(image_simhash_as_int)

    uf = unionfind.UnionFind(simhash_to_creative_ids.keys())
    # Process all image sim hashes to get clusters of similar image simhashes
    for curr_simhash in simhash_to_creative_ids:
        found = image_simhash_tree.find(curr_simhash, BIT_DIFFERENCE_THRESHOLD)
        # BKTree.find returns tuples of form (bit difference, value)
        found_hashes = [x[1] for x in found]
        # Connect all combinantions (regardless of order) of found simhashes
        for hash_pair in itertools.combinations(found_hashes, 2):
            uf.union(hash_pair[0], hash_pair[1])

    components = uf.components()
    logging.info('Processed %d creative IDs. got %d clusters', num_processed, len(components))
    # TODO(macpd): Create map of cluster -> creative IDs

    return components
