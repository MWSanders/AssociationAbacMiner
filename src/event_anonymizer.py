import ipaddress
import random
import uuid

from pymongo import MongoClient, InsertOne, UpdateOne, ReplaceOne
from src.model.EnvLogUniverseGenerator import r_valid_fields
from src.config import config, config_anon
from src.model import event_flattner

client = MongoClient('mongodb://127.0.0.1:27017', connectTimeoutMS=config.mongodb_timeout)
calp = client.calp
events_collection = calp.events

anon_client = config_anon.anon_client
calp_anon = anon_client.calp_anon
events_anon_collection = calp_anon.events_resources

valid_keys = r_valid_fields
anonymized_keys= {
    "uncorrelated_strings":["userIdentity_userName", "userIdentity_accessKeyId"],
    "ips": ["sourceIPAddress"]
}

for key in valid_keys:
    if key.startswith('requestParameters_') or key.startswith('additionalEventData'):
        anonymized_keys['uncorrelated_strings'].append(key)

replaced_values = {k:{} for k in anonymized_keys["uncorrelated_strings"]}
replaced_values.update({k:{} for k in anonymized_keys["ips"]})
replaced_octets = {}


def rset_value(result, k, value):
    if '_' not in k:
        result[k] = value.replace('__', '.')
        return
    parts = k.split('_')
    if parts[0] not in result:
        result[parts[0]] = {}
    new_k = '_'.join(parts[1:])
    rset_value(result[parts[0]], new_k, value)

def unflatten_event(event):
    result = {}
    for k, v in event.items():
        key_paths = k.split('_')
        if len(key_paths) == 1:
            result[k] = v.replace('__', '.')
            continue
        rset_value(result, k, v)
    return result


record_counter = 0
requests = []
for event in events_collection.find({}):
    fields_to_remove = set()
    flattened_event = event_flattner.flatten(event)
    for field, v in flattened_event.items():
        if field not in valid_keys:
            fields_to_remove.add(field)
        elif field in anonymized_keys['uncorrelated_strings']:
            if field in replaced_values:
                if v in replaced_values[field]:
                    flattened_event[field] = replaced_values[field][v]
                else:
                    anonymized_value = str(uuid.uuid4())
                    replaced_values[field][v] = anonymized_value
                    flattened_event[field] = anonymized_value
        elif field in anonymized_keys['ips']:
            try:
                test = ipaddress.ip_address(v.replace('__','.'))
            except ValueError:
                continue
            if field in replaced_values:
                if v in replaced_values[field]:
                    flattened_event[field] = replaced_values[field][v]
                else:
                    orig_octets = v.split('__')
                    new_octects = []
                    for octet in orig_octets:
                        if octet in replaced_octets:
                            new_octects.append(replaced_octets[octet])
                        else:
                            while True:
                                new_octect = str(random.randint(0,9999))
                                if new_octect not in replaced_octets:
                                    break
                            replaced_octets[octet] = new_octect
                            new_octects.append(new_octect)
                    new_octects = '__'.join(new_octects)
                    replaced_values[field][v] = new_octects
                    flattened_event[field] = new_octects
    for field in fields_to_remove:
        flattened_event.pop(field)
    unflatten_evented = unflatten_event(flattened_event)
    unflatten_evented['_id'] = event['_id']
    unflatten_evented['eventTime'] = event['eventTime']
    requests.append(ReplaceOne(filter={'_id': unflatten_evented['_id']}, replacement=unflatten_evented, upsert=True))
    if len(requests) >= 1000:
        events_anon_collection.bulk_write(requests, ordered=False)
        requests.clear()
    record_counter += 1
    if record_counter % 100000 == 0:
        print('Inserted %d anonymized records' % record_counter)
if requests:
    events_anon_collection.bulk_write(requests, ordered=False)
    requests.clear()
