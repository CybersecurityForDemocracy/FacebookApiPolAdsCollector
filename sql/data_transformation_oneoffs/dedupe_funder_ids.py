import configparser
import logging
import sys
import time

import psycopg2
import psycopg2.extras

logging.basicConfig(handlers=[logging.FileHandler("sql/dedupe_funder_ids.log"),
                              logging.StreamHandler()],
                    format='[%(levelname)s\t%(asctime)s] {%(pathname)s:%(lineno)d} %(message)s',
                    level=logging.INFO)

def get_database_connection(config):
  host = config['POSTGRES']['HOST']
  dbname = config['POSTGRES']['DBNAME']
  user = config['POSTGRES']['USER']
  password = config['POSTGRES']['PASSWORD']
  port = config['POSTGRES']['PORT']

  db_authorize = "host=%s dbname=%s user=%s password=%s port=%s" % (host, dbname, user, password, port)
  connection = psycopg2.connect(db_authorize)
  logging.info('Established connecton to %s', connection.dsn)
  return connection

def get_distint_funder_names_with_id(db_connection):
  cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
  query = ('SELECT funder_name, min(funder_id) as min_funder_id FROM '
        'funder_metadata GROUP BY funder_name;')
  cursor.execute(cursor.mogrify(query))
  logging.info(cursor.query)
  funder_name_to_id = {}
  for row in cursor:
    funder_name_to_id[row['funder_name']] = row['min_funder_id']
  return funder_name_to_id

def get_all_ids_for_funder_name(db_connection, funder_name):
  cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
  if funder_name:
    query = ('SELECT funder_id FROM funder_metadata WHERE funder_name = %s')
    psycopg2.extras.execute_values(cursor, query, [(funder_name,)])
  else:
    cursor.execute(cursor.mogrify('SELECT funder_id FROM funder_metadata WHERE funder_name is NULL'))
  logging.info(cursor.query)
  return set([row['funder_id'] for row in cursor.fetchall()])

def update_all_ad_metadata_to_canonical_id(db_connection, canonical_id,
      ids_to_update):
  logging.info('Updating ids for canonical funder_id: %s', canonical_id)
  cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
  update_query = ('UPDATE ad_metadata SET funder_id = %s WHERE funder_id in (%s)' %
        (canonical_id,
        ','.join([str(i) for i in ids_to_update])))
  cursor.execute(cursor.mogrify(update_query))
  logging.info(cursor.query)
  logging.info('Updated %d rows', cursor.rowcount)

def remove_undesired_funder_ids(db_connection, ids_to_delete):
  logging.info('Deleting funder IDs: %r', ids_to_delete)
  cursor = db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
  delete_query = ('DELETE FROM funder_metadata WHERE funder_id IN (%s)' %
        ','.join([str(i) for i in ids_to_delete]))
  cursor.execute(cursor.mogrify(delete_query))
  logging.info(cursor.query)
  logging.info('Deleted %d rows', cursor.rowcount)


def dedup_all_funder_ids(db_connection):
  logging.info('Deduping all unneeded funder_ids')
  funder_name_to_canonical_id = get_distint_funder_names_with_id(db_connection)
  total_funder_names_to_process = len(funder_name_to_canonical_id)
  num_processed = 0
  for funder_name in funder_name_to_canonical_id:
    canonical_id = funder_name_to_canonical_id[funder_name]
    logging.info('Deduping ids for funder_name: %s canonical_id: %s',
        funder_name, canonical_id)
    all_ids_for_funder = get_all_ids_for_funder_name(db_connection, funder_name)
    all_ids_for_funder.remove(canonical_id)
    if not all_ids_for_funder:
      logging.info('funder_name: %s has no IDs to dedupe. SKIPPING',
          funder_name)
      continue

    logging.info('funder_name: %s has %d IDs to dedupe.', funder_name,
        len(all_ids_for_funder))
    # remove canonical ID from set so we don't overwite it
    # update all refs
    update_all_ad_metadata_to_canonical_id(db_connection, canonical_id,
        all_ids_for_funder)
    remove_undesired_funder_ids(db_connection, all_ids_for_funder)
    db_connection.commit()
    num_processed += 1
    logging.info('Proccessed %d of %d funder_names.', num_processed,
        total_funder_names_to_process)




def main(argv):
  config = configparser.ConfigParser()
  config.read(argv[0])
  try:
    with get_database_connection(config) as db_connection:
      dedup_all_funder_ids(db_connection)

  finally:
    db_connection.commit()
    db_connection.close()

if __name__ == '__main__':
  if len(sys.argv) < 2:
    sys.exit('Usage: %s <config file>' % sys.argv[0])
  main(sys.argv[1:])
