import collections
import itertools
import logging

import dhash
import pybktree
import simhash

import config_utils
import db_functions
import snapshot_url_util
from lib.unionfind import unionfind

BIT_DIFFERENCE_THRESHOLD = 3

AdCreativeClusterRecord = collections.namedtuple('AdCreativeClusterRecord', ['ad_creative_id',
                                                                             'ad_cluster_id'])
CreativeIDAndSimHash = collections.namedtuple('CreativeIDAndSimHash', ['creative_id', 'sim_hash'])

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

def _ad_creative_body_text_similarity_clusters(database_connection_params, existing_clusters_union_find):
    """Adds clusters of creative IDs with similar ad creative body text simhashes to
    existing_clusters_union_find

    Args:
        database_connection_params: config_utils.DatabaseConnectionParams params to connect to
            database from which to retrieve ad creatives.
    """
    with config_utils.get_database_connection(database_connection_params) as db_connection:
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
    logging.info('Have %d text simhashes to process.', len(simhash_to_creative_ids))
    for curr_simhash_as_int in simhash_to_creative_ids:
        found = text_simhash_index.get_near_dups(simhash.Simhash(curr_simhash_as_int))
        # Convert found creative IDs back to ints since SimhashIndex converts returns them as
        # strings regardless of the provided type.
        found = [int(x) for x in found]
        # Connect all combinantions (regardless of order) of found simhashes
        for creative_id_pair in itertools.combinations(found, 2):
            existing_clusters_union_find.union(creative_id_pair[0], creative_id_pair[1])

def get_num_bits_different(creative_id_and_simhash1, creative_id_and_simhash2):
    return dhash.get_num_bits_different(creative_id_and_simhash1.sim_hash,
                                        creative_id_and_simhash2.sim_hash)


def _ad_creative_image_similarity_clusters(database_connection_params, existing_clusters_union_find):
    """Adds clusters of creative IDs with similar image simhashes to existing_clusters_union_find

    Args:
        database_connection_params: config_utils.DatabaseConnectionParams params to connect to
            database from which to retrieve ad creatives.
    """
    with config_utils.get_database_connection(database_connection_params) as db_connection:
        db_interface = db_functions.DBInterface(db_connection)

        # Get all ad creative images simhashes from database.
        creative_id_to_simhash = db_interface.all_ad_creative_image_simhashes()

    # Create BKTree with dhash bit difference function as distance_function, used to find similar
    # hashes
    image_simhash_tree = pybktree.BKTree(get_num_bits_different)

    # Create inverse map of simhash -> set of creative ID(s), and normalize creative_id_to_simhash
    # values to ints.
    simhashes_for_clustering = set()
    for creative_id in creative_id_to_simhash:
        image_simhash_as_int = int(creative_id_to_simhash[creative_id], 16)
        simhashes_for_clustering.add(image_simhash_as_int)
        creative_id_and_simhash = CreativeIDAndSimHash(int(creative_id), image_simhash_as_int)
        image_simhash_tree.add(creative_id_and_simhash)

    # Process all image sim hashes to get clusters of similar image simhashes
    num_simhash_processed = 0
    connections = set()
    logging.info('Have %d image simhashes to process.', len(simhashes_for_clustering))
    for curr_simhash in simhashes_for_clustering:
        num_simhash_processed += 1
        # We create a fake CreativeIDAndSimHash with ID -1, but the current
        found = image_simhash_tree.find(CreativeIDAndSimHash(-1, curr_simhash), BIT_DIFFERENCE_THRESHOLD)
        if num_simhash_processed % 1000 == 0:
            logging.info('Processed %d image simhashses.', num_simhash_processed)
        # BKTree.find returns tuples of form (bit difference, value). This extracts a set of all
        # creative IDs found.
        found_creative_ids = set([x[1].creative_id for x in found])
        for creative_id_pair in itertools.combinations(found_creative_ids, 2):
            existing_clusters_union_find.union(creative_id_pair[0], creative_id_pair[1])

def _get_lowest_creative_id_cluster_id(existing_ad_creative_id_to_ad_cluster_id, creative_id_set):
    creative_id_set = creative_id_set.copy()
    while creative_id_set:
        min_creative_id = min(creative_id_set)
        if min_creative_id in existing_ad_creative_id_to_ad_cluster_id:
            return existing_ad_creative_id_to_ad_cluster_id[min_creative_id]
        creative_id_set.remove(min_creative_id)

    return None


def update_ad_creative_clusters(database_connection_params):
    """Find all clusters of ad creatives which have similar text or image simhashes, update cluster
    data in databases.

    Args:
        database_connection_params: config_utils.DatabaseConnectionParams params for connecting to
        database.
    Returns:
        Clusters of ad creative IDs with similar text and images.
    """
    all_clusters_union_find = unionfind.UnionFind()
    logging.info('Starting text clustering')
    _ad_creative_body_text_similarity_clusters(database_connection_params, all_clusters_union_find)
    components = all_clusters_union_find.components()
    logging.info('Got %d text clusters', len(components))
    logging.info('Starting image cluster. Passing in text clusters.')
    _ad_creative_image_similarity_clusters(database_connection_params, all_clusters_union_find)
    components = all_clusters_union_find.components()
    logging.info('Got %d text image clusters', len(components))
    with config_utils.get_database_connection(database_connection_params) as db_connection:
        db_interface = db_functions.DBInterface(db_connection)
        existing_ad_creative_id_to_ad_cluster_id = db_interface.existing_ad_clusters()
        if existing_ad_creative_id_to_ad_cluster_id:
            next_new_cluster_id = max(existing_ad_creative_id_to_ad_cluster_id.values())
        else:
            next_new_cluster_id = 0


        ad_creative_cluster_records = []
        for component in components:
            cluster_id = _get_lowest_creative_id_cluster_id(
                existing_ad_creative_id_to_ad_cluster_id, component)
            if cluster_id is None:
                cluster_id = next_new_cluster_id
                next_new_cluster_id += 1
            for creative_id in component:
                ad_creative_cluster_records.append(AdCreativeClusterRecord(ad_creative_id=int(creative_id),
                                                                           ad_cluster_id=cluster_id))

        logging.info('Inserting/updating %d Ad cluster records in DB.', len(ad_creative_cluster_records))
        db_interface.insert_or_update_ad_cluster_records(ad_creative_cluster_records)
        return components
