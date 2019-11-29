from pymongo import MongoClient
from src.config import config
from src.job import job_utls
from src.experiment import FeatureSelector
from collections import Counter
import json

client = MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
db = client.calp
events_collection = db.events
jobs_collection = db.abac_job_queue

job = jobs_collection.find_one(filter={'_id': '2017-09-30_to_2017-10-30-819nwQtosi'})
job_utls.decode_query_dates(job)

key_universe = Counter()
query = job['perm_universe_query']
records_count = events_collection.count(query)
for event in events_collection.find(query):
    flat_event = FeatureSelector.flatten(event, "_")
    key_universe.update(flat_event.keys())

job_utls.encode_query_dates(job)
keys = {'records_count': records_count, 'keys_count': len(key_universe), 'keys': key_universe}
with open('key_universe.json', 'w') as outfile:
    json.dump(keys, outfile)
print(key_universe)