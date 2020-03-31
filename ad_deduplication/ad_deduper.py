"""Module to dedupe ads into clusters based via text and image clustering based on hash simialirty."""
import collections
import itertools
import logging
import sys

import dhash
import pybktree
import simhash

import config_utils
import db_functions
from lib.unionfind import unionfind

BIT_DIFFERENCE_THRESHOLD = 3

AdClusterRecord = collections.namedtuple('AdClusterRecord', ['archive_id', 'ad_cluster_id'])
ArchiveIDAndSimHash = collections.namedtuple('ArchiveIDAndSimHash', ['archive_id', 'sim_hash'])

def _ad_creative_body_text_similarity_clusters(database_connection, existing_clusters_union_find):
    """Adds clusters of archive IDs with similar ad creative body text simhashes to
    existing_clusters_union_find

    Args:
        database_connection: psycopg2.connection to connect to database from which to retrieve ad
        creative data.
    """
    db_interface = db_functions.DBInterface(database_connection)

    # Get all ad creative body simhashes from database.
    raw_archive_id_to_simhash = db_interface.all_ad_creative_text_simhashes()

    # Convert map to str id -> Simhash obj
    archive_id_to_simhash = {}
    simhash_to_archive_ids = collections.defaultdict(set)
    for archive_id in raw_archive_id_to_simhash:
        text_simhash_as_int = int(raw_archive_id_to_simhash[archive_id], 16)
        simhash_to_archive_ids[text_simhash_as_int].add(archive_id)
        archive_id_to_simhash[str(archive_id)] = simhash.Simhash(text_simhash_as_int)

    # Create simhash index
    text_simhash_index = simhash.SimhashIndex(archive_id_to_simhash.items(),
                                              k=BIT_DIFFERENCE_THRESHOLD)

    # Process all simhashes to get clusters of archive_ids with similar text
    logging.info('Have %d text simhashes to process.', len(simhash_to_archive_ids))
    for curr_simhash_as_int in simhash_to_archive_ids:
        found = text_simhash_index.get_near_dups(simhash.Simhash(curr_simhash_as_int))
        # Convert found creative IDs back to ints since SimhashIndex converts returns them as
        # strings regardless of the provided type.
        found = [int(x) for x in found]
        # Connect all combinantions (regardless of order) of found simhashes
        for archive_id_pair in itertools.combinations(found, 2):
            existing_clusters_union_find.union(archive_id_pair[0], archive_id_pair[1])

def get_num_bits_different(archive_id_and_simhash1, archive_id_and_simhash2):
    return dhash.get_num_bits_different(archive_id_and_simhash1.sim_hash,
                                        archive_id_and_simhash2.sim_hash)


def _ad_creative_image_similarity_clusters(database_connection, existing_clusters_union_find):
    """Adds clusters of creative IDs with similar image simhashes to existing_clusters_union_find

    Args:
        database_connection: psycopg2.connection to connect to database from which to retrieve ad
        creative data.
    """
    db_interface = db_functions.DBInterface(database_connection)

    # Get all ad creative images simhashes from database.
    archive_id_to_simhash = db_interface.all_ad_creative_image_simhashes()

    # Create BKTree with dhash bit difference function as distance_function, used to find similar
    # hashes
    image_simhash_tree = pybktree.BKTree(get_num_bits_different)

    # Create inverse map of simhash -> set of creative ID(s), and normalize archive_id_to_simhash
    # values to ints.
    simhashes_for_clustering = set()
    for archive_id in archive_id_to_simhash:
        image_simhash_as_int = int(archive_id_to_simhash[archive_id], 16)
        simhashes_for_clustering.add(image_simhash_as_int)
        archive_id_and_simhash = ArchiveIDAndSimHash(int(archive_id), image_simhash_as_int)
        image_simhash_tree.add(archive_id_and_simhash)

    # Process all image sim hashes to get clusters of similar image simhashes
    num_simhash_processed = 0
    logging.info('Have %d image simhashes to process.', len(simhashes_for_clustering))
    for curr_simhash in simhashes_for_clustering:
        num_simhash_processed += 1
        # We create a fake ArchiveIDAndSimHash with ID -1, but the current
        found = image_simhash_tree.find(ArchiveIDAndSimHash(-1, curr_simhash), BIT_DIFFERENCE_THRESHOLD)
        if num_simhash_processed % 1000 == 0:
            logging.info('Processed %d image simhashses.', num_simhash_processed)
        # BKTree.find returns tuples of form (bit difference, value). This extracts a set of all
        # creative IDs found.
        found_archive_ids = {x[1].archive_id for x in found}
        for archive_id_pair in itertools.combinations(found_archive_ids, 2):
            existing_clusters_union_find.union(archive_id_pair[0], archive_id_pair[1])

def _get_lowest_archive_id_cluster_id(existing_ad_archive_id_to_ad_cluster_id, archive_id_set):
    """Get cluster ID of lowest value archive ID present in existing ad_clusters table.

    Args:
        existing_ad_archive_id_to_ad_cluster_id: dict archive_id -> ad_cluster_id from database.
        archive_id_set: set of archive_id in a cluster.
    Returns:
        int ad_cluster_id of lowest value archive ID present in existing ad_clusters table. None if
        no elements of archive_id_set are present in database results.
    """
    archive_id_set = archive_id_set.copy()
    while archive_id_set:
        min_archive_id = min(archive_id_set)
        if min_archive_id in existing_ad_archive_id_to_ad_cluster_id:
            return existing_ad_archive_id_to_ad_cluster_id[min_archive_id]
        archive_id_set.remove(min_archive_id)

    return None


def update_ad_clusters(database_connection):
    """Find all clusters of ads which have similar text or image simhashes, update cluster data in
    databases.

    Args:
        database_connection: psycopg2.connection for connecting to database.
    Returns:
        Clusters of archive IDs with similar text and images.
    """
    with database_connection:
        all_clusters_union_find = unionfind.UnionFind()
        logging.info('Starting text clustering')
        _ad_creative_body_text_similarity_clusters(database_connection, all_clusters_union_find)
        components = all_clusters_union_find.components()
        logging.info('Got %d text clusters', len(components))
        logging.info('Starting image cluster. Passing in text clusters.')
        _ad_creative_image_similarity_clusters(database_connection, all_clusters_union_find)
        components = all_clusters_union_find.components()
        logging.info('Got %d text image clusters', len(components))
        db_interface = db_functions.DBInterface(database_connection)
        existing_ad_archive_id_to_ad_cluster_id = db_interface.existing_ad_clusters()
        existing_cluster_ids = set()
        if existing_ad_archive_id_to_ad_cluster_id:
            next_new_cluster_id = max(existing_ad_archive_id_to_ad_cluster_id.values()) + 1
            existing_cluster_ids = set(existing_ad_archive_id_to_ad_cluster_id.values())
        else:
            next_new_cluster_id = 0

        allocated_cluster_ids = set()
        ad_cluster_records = []
        for component in components:
            cluster_id = _get_lowest_archive_id_cluster_id(
                existing_ad_archive_id_to_ad_cluster_id, component)
            if cluster_id is None:
                cluster_id = next_new_cluster_id
                next_new_cluster_id += 1
            for archive_id in component:
                ad_cluster_records.append(AdClusterRecord(archive_id=int(archive_id),
                                                          ad_cluster_id=cluster_id))
            allocated_cluster_ids.add(cluster_id)

        orphaned_cluster_ids = existing_cluster_ids - allocated_cluster_ids
        if orphaned_cluster_ids:
            logging.info('Orphaned cluster ID(s): %s', orphaned_cluster_ids)
        logging.info('Inserting/updating %d Ad cluster records in DB.', len(ad_cluster_records))
        db_interface.insert_or_update_ad_cluster_records(ad_cluster_records)
        db_interface.ad_clusters_spend_and_impression_sums()
        database_connection.commit()
        return components

def main(config_path):
    config = config_utils.get_config(config_path)
    database_connection = config_utils.get_database_connection_from_config(config)
    update_ad_clusters(database_connection)


if __name__ == '__main__':
    config_utils.configure_logger("ad_deduper.log")
    main(sys.argv[1])
