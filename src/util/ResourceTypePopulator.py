from src.config import config
import collections

client = config.client #MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
db = client.calp
events_collection = config.events_collection #db.events
type_collection = config.service_op_resource_collection #db.ServiceOpResourceTypes
type_collection.remove({})

service_op_resources = {}
query = {'errorCode': {'$exists': False}}
records = 0
for event in events_collection.find(query):
    records += 1
    if records % 100000 == 0:
        print('Records: %d' % records)
    if 'errorCode' in event:
        continue
    service = event['eventSource'].split('.')[0]
    op = event['eventName']
    resources = event['meta_normalizedResourcesPrimary'] if 'meta_normalizedResourcesPrimary' in event else None
    if service not in service_op_resources:
        service_op_resources[service] = collections.OrderedDict()
    op_resources = service_op_resources[service]
    if op not in op_resources:
        op_resources[op] = set()
    resource_types = op_resources[op]
    inner_set = set()
    if resources is not None:
        for resource in resources:
            arn_parts = resource.split(":")
            resource_type = arn_parts[5].split('/')[0]
            inner_set.add(resource_type)
    else:
        inner_set.add("None")
    inner_list = [x for x in inner_set]
    resource_types.add('_'.join(i for i in sorted(inner_list)))
print(service_op_resources)
for service, actions in service_op_resources.items():
    # for action, typeset in actions.items():
    #     actions[action] = [list(x) for x in typeset]
    service_op_resources[service] = collections.OrderedDict(sorted(actions.items()))
    for op, types in service_op_resources[service].items():
        service_op_resources[service][op] = list(service_op_resources[service][op])
    record = service_op_resources[service]
    record['_id'] = service
    type_collection.insert_one(record)
