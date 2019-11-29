import traceback

import dateutil.parser
import six
from Orange.data import *
from src.model import RuleUtils
from orangecontrib.associate.fpgrowth import *
from pymongo import MongoClient
from src.config import config
from collections import Counter
# from package.src.mongo_proxy.mongodb_proxy import MongoProxy
client = MongoClient('mongodb://127.0.0.1:27017', connectTimeoutMS=40000)
db = client.calp
events_collection = db.sfi_events




# def build_orange_table_from_mongo_query(mongo_query, valid_keys=None, prune_null_resources=True, all_logs_index='flat-all-log-entries', unique_logs_index='flat-unique-log-entries'):
#     field_values = {}
#     single_value_columns = set() #TODO return values that are always true, add them to Rules
#     records = 0
#     key_value_counter = Counter()
#     paginator = helpers.scan(es, query={"query": {"match_all": {}}}, index=unique_logs_index, doc_type='doc')
#     for hit in paginator:
#         records += 1
#         # if records % 1000 == 0:
#         #     print('Records : ' + str(records))
#         for key, value in hit['_source'].items():
#             if key == '_id' or (valid_keys and key not in valid_keys):
#                 continue
#             RuleUtils.addMulti(field_values, key, value)
#             key_value_counter.update(['%s=%s' % (key, value)])
#
#     for k, v in dict(key_value_counter).items():
#         if v == records:
#             single_value_columns.add(k)  # ignore fields that always have the same value
#             field_name = k.split('=')[0]
#             field_values.pop(field_name)
#
#     orange_columns = []
#     for key, value in field_values.items():
#         # if len(value) == 1 and records > 1:
#         #     single_value_columns.add('%s=%s' % (key, value.pop()))#ignore fields that always have the same value
#         #     continue
#         for elem in value:
#             if not isinstance(elem, str):
#                 value.remove(elem)
#                 value.add(str(elem))
#         try:
#             column = DiscreteVariable(key, values=value)
#         except Exception as ex:
#             traceback.print_exc()
#             print(value)
#         orange_columns.append(column)
#     resource_encoder = None
#     domain = Domain(orange_columns)
#
#     records = 0
#     table = Table(domain)
#     paginator = helpers.scan(es, query={"query": {"match_all": {}}}, index=all_logs_index, doc_type='doc')
#     for hit in paginator:
#         instance = createInstance(domain, hit['_source'], resource_encoder, prune_null_resources)
#         table.append(instance)
#         records += 1
#         # if records % 1000 == 0:
#         #     print('Records : ' + str(records))
#     # print('Built Table: %d recrods' % len(table))
#     return table, single_value_columns

user_service_counter = {}
user_service_op_counter = {}
service_user_counter = {}
service_op_user_counter = {}
service_counter = Counter()
user_counter = Counter()
op_counter = Counter()
usernames = list(events_collection.find({}).distinct('userIdentity.userName'))
eventsources = [s.split('.')[0] for s in events_collection.find({}).distinct('eventSource')]
eventnames = events_collection.find({}).distinct('eventName')

roles = [('IAM', ['iam'], ['user1', 'user2', 'user3'])]
events = 0
for event in events_collection.find({}, {'userIdentity.userName':1, 'eventSource':1, 'eventName':1}):
    username = event['userIdentity']['userName']
    event_source = event['eventSource'].split('.')[0]
    event_name = event_source + ':' + event['eventName']
    if event_source.startswith('signin'):
        continue
    # DEBUGING====
    if not event_source.startswith('iam'):
        continue
    if event['eventName'].startswith('List') or event['eventName'].startswith('Get'):
        continue
    #=============
    if event_source not in service_user_counter:
        service_user_counter[event_source] = Counter()
    if event_source not in service_op_user_counter:
        service_op_user_counter[event_source] = {}
    if event_name not in service_op_user_counter[event_source]:
        service_op_user_counter[event_source][event_name] = Counter()
    if username not in user_service_counter:
        user_service_counter[username] = Counter()
    if username not in user_service_op_counter:
        user_service_op_counter[username] = {}
    if event_source not in user_service_op_counter[username]:
        user_service_op_counter[username][event_source] = Counter()
    user_counter.update([username])
    op_counter.update([event_name])
    service_counter.update([event_source])
    service_user_counter[event_source].update([username])
    service_op_user_counter[event_source][event_name].update([username])
    user_service_counter[username].update([event_source])
    user_service_op_counter[username][event_source].update([event_name])
    events += 1
    if events == 300000:
        break

# print(service_counter.most_common(len(service_counter)))
# for event_source,v in service_counter.most_common(len(service_counter)):
#     print('%s: %d' % (event_source,v))
#     for username, user_counter_ in service_user_counter[event_source].items():
#         print('\t%s: %s' % (username,user_counter_))
# print(service_user_counter)

X = np.zeros((len(eventsources), len(usernames)))
# print('%s, %s' % ('eventSource', ', '.join(usernames)))
for i in range(0, len(eventsources)):
    row = X[i]
    eventsource = eventsources[i]
    for j in range(0, len(usernames)):
        username = usernames[j]
        if eventsource in service_user_counter and username in service_user_counter[eventsource]:
            row[j] = 1
for event_source,v in service_counter.most_common(len(service_counter)):
    service_user_list = []
    for username in usernames:
        if username in service_user_counter[event_source]:
            service_user_list.append('True')
        else:
            service_user_list.append('')
    print('%s, %s' % (event_source, ', '.join(service_user_list)))


itemsets = dict(frequent_itemsets(X, 0.20))
sorted_itemsets = [(k, itemsets[k]) for k in sorted(itemsets, key=itemsets.get, reverse=True)]
sorted_itemsets.sort(key=lambda x: len(x[0]), reverse=True)
print(itemsets)
for items, count in sorted_itemsets:
    users = []
    shared_services = set()
    for user_id in items:
        username = usernames[user_id]
        if not shared_services:
            shared_services = set(user_service_counter[username].keys())
        else:
            shared_services.intersection_update(user_service_counter[username].keys())
        users.append(username)
    print('%s (%d) ==> %s' % (', '.join(users), count, shared_services))

print('\n============\n')

X = np.zeros((len(usernames), len(eventsources)))
for i in range(0, len(usernames)):
    row = X[i]
    username = usernames[i]
    for j in range(0, len(eventsources)):
        eventsource = eventsources[j]
        if username in user_service_counter and eventsource in user_service_counter[username]:
            row[j] = 1
itemsets = dict(frequent_itemsets(X, 0.35))
sorted_itemsets = [(k, itemsets[k]) for k in sorted(itemsets, key=itemsets.get, reverse=True)]
sorted_itemsets.sort(key=lambda x: len(x[0]), reverse=True)
for items, count in sorted_itemsets:
    services = []
    shared_users = set()
    for service_id in items:
        servicename = eventsources[service_id]
        if not shared_users:
            shared_users = set(service_user_counter[servicename].keys())
        else:
            shared_users.intersection_update(service_user_counter[servicename].keys())
        services.append(servicename)
    print('%s (%d) ==> %s' % (', '.join(services), count, shared_users))
print()