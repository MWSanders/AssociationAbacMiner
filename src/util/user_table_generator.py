import os, json
import dateutil.parser
from bson import json_util
from src.eval.WscCalculator import WscCalculator
from src.model.EnvParamGenerators import *
from src.util.AtomicCounter import AtomicCounter
from multiprocessing.pool import Pool
from src.job import job_utls
import statistics
client = config.client
events_collection = config.events_collection

start = dateutil.parser.parse('2017-03-21 00:00:00.000Z')
end = dateutil.parser.parse('2018-07-12 00:00:00.000Z')
query = {"eventTime": {"$gte": start,"$lte": end}}
# query = json.dumps(query, default=json_util.default)
# query = job_utls.replace_query_epoch_with_datetime(query)
# Get list of users
users = events_collection.distinct("userIdentity.userName", query)
print('Users: %d' % len(users))

user_services = []
for user in users:
    query['userIdentity.userName'] = user
    eventSources = events_collection.distinct("eventSource", query)
    # print(json.dumps(query, default=json_util.default))
    # print('userName: %s, eventSources: %d' % (user, len(eventSources)))
    user_services.append(len(eventSources))
print('service avg: %f, stddev: %f' % (statistics.mean(user_services), statistics.stdev(user_services)))

user_actions = []
for user in users:
    query['userIdentity.userName'] = user
    eventNames = events_collection.distinct("eventName", query)
    # print(json.dumps(query, default=json_util.default))
    # print('userName: %s, eventSources: %d' % (user, len(eventSources)))
    user_actions.append(len(eventNames))

print('avg: %f, stddev: %f' % (statistics.mean(user_actions), statistics.stdev(user_actions)))

user_action_counts = []
for user in users:
    query['userIdentity.userName'] = user
    event_count = events_collection.find(query).count()
    user_action_counts.append(event_count)

print('avg: %f, stddev: %f' % (statistics.mean(user_action_counts), statistics.stdev(user_action_counts)))