from pymongo import MongoClient
from src.config import config
from src.model import RuleUtils
import json
from Orange.data import *
from orangecontrib.associate.fpgrowth import *
from collections import *

query = config.mongo_source_query
client = MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
db = client.calp
events_collection = db.events

if __name__ == "__main__":
    query = config.mongo_source_query
    events = events_w_resources = multiple_same_type = 0
    c = Counter()
    for event in events_collection.find(query):
        events += 1
        event_types = set()
        if 'meta_normalizedResourcesPrimary' in event:
            events_w_resources += 1
            resources = event['meta_normalizedResourcesPrimary']
            for resource in resources:
                type = resource.split(':')[5].split('/')[0]
                if type in event_types:
                    multiple_same_type += 1
                event_types.add(type)
            c.update(str(len(event_types)))
    print('Events: %d' % events)
    print('Events with Resources: %d' % events_w_resources)
    print('Events with multiple resources of same type: %d' % multiple_same_type)
    print(c)