import copy
import json
from datetime import datetime
from bson import json_util

from pymongo import MongoClient

from bson import json_util
from src.LogUniverseBuilder import LogUniverseBuilder
from src.config import config
from src.eval.RuleEvaluator import RuleEvaluator
from src.model.EventNormalizerNg import EventNormalizerNg
from src.model.ParamGeneratorsNg import ResourceGeneratorNg
from src.job import job_utls
import os
import dateutil.parser
import logging
import hashlib
import base64
from collections import Counter

client = MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
db = client.calp
event_colleciton = db.events
attrib_universe_collction = db.abac_possible_params

class EventTimeBucketCoutner(object):
    def __init__(self, use_resources=False):
        self.calendar_start = '2017-09-30 00:00:00.000Z'
        self.calendar_end = '2017-12-14 23:59:59.000Z'

        self.source_query = {"eventTime": {'$gte': dateutil.parser.parse(self.calendar_start),
                                      '$lte': dateutil.parser.parse(self.calendar_end)},
                        'userIdentity.type': 'IAMUser'
                        }

        counter = Counter()
        nightowl_counter = Counter()
        nightowl_services = Counter()
        nightowl_operations = Counter()
        nightowl_month_day = Counter()
        hours_per_bucket = 4
        # hour_bucket = str(int(eventTime.hour / hours_per_bucket))
        for event in event_colleciton.find(self.source_query): #, {"eventTime":1, "_id":1}):
            eventTime = event['eventTime']
            # if eventTime.hour >= 5 and eventTime.hour <= 10 and event['userIdentity']['userName'] == 'talford':
            #     nightowl_counter.update([event['userIdentity']['userName']])
            #     nightowl_services.update([event['eventSource']])
            #     nightowl_operations.update([event['eventName']])
            #     nightowl_month_day.update(['%d/%d' % (eventTime.month, eventTime.day)])
            #     print(event)
            counter.update([eventTime.hour])

        # print('user, events')
        # user_dict = dict(nightowl_counter)

        print('hour, events')
        hour_dict = dict(counter)
        for hour in sorted(hour_dict.keys()):
            print('%d, %d' % (hour, hour_dict[hour]))




if __name__ == "__main__":
    gen = EventTimeBucketCoutner(False)
    # id = gen.store_param_universe()
    # print('User: %d, Op: %d, Total: %d' % (gen.user_possible_privs, gen.op_possible_privs, gen.total_possible_priv_states))
    #
    # record = attrib_universe_collction.find_one({'_id': id})
    # test_log_universe = LogUniverseBuilder(None, None, record)
