import datetime
import json
from bson import json_util

def replace_query_epoch_with_datetime(query):
    if isinstance(query, str):
        query = json.loads(query)
    start_date = datetime.datetime.utcfromtimestamp(query['eventTime']['$gte']['$date'] /1000)
    query['eventTime']['$gte'] = start_date
    if isinstance(query, str):
        query = json.loads(query )
    end_date = datetime.datetime.utcfromtimestamp(query['eventTime']['$lte']['$date'] /1000)
    query['eventTime']['$lte'] = end_date
    return query


def decode_query_dates(job):
    job['obs_query'] = replace_query_epoch_with_datetime(json.loads(job['obs_query']))
    job['opr_query'] = replace_query_epoch_with_datetime(json.loads(job['opr_query']))
    job['obs_opr_query'] = replace_query_epoch_with_datetime(json.loads(job['obs_opr_query']))
    job['perm_universe_query'] = replace_query_epoch_with_datetime(json.loads(job['perm_universe_query']))
    return job

def encode_query_dates(job):
    job['obs_query'] = json.dumps(job['obs_query'], default=json_util.default)
    job['opr_query'] = json.dumps(job['opr_query'], default=json_util.default)
    job['obs_opr_query'] = json.dumps(job['obs_opr_query'], default=json_util.default)
    job['perm_universe_query'] = json.dumps(job['perm_universe_query'], default=json_util.default)
    return job
