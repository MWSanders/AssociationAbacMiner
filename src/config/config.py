import logging
import socket
import os
from pymongo import MongoClient
from elasticsearch import Elasticsearch

#MongoDB Settings
mongodb_conn = 'mongodb://127.0.0.1:27017'
client = MongoClient(mongodb_conn, connectTimeoutMS=40000, maxPoolSize=8)
db = client.calp
param_universe_info_collection = db.abac_param_universe_info
jobs_collection = db.abac_job_queue
scores_collection = db.abac_scores
policy_colleciton = db.abac_policies
summary_collection = db.abac_aggregate_scores
service_op_resource_collection = db.ServiceOpResourceTypes
events_collection = db.events #db.events_resources
dynamic_params_cache = db.dynamic_params_cache
mongodb_timeout = 40000

#ElasticSearch settings
es_connect_string = 'http://localhost:9200'
es = Elasticsearch([es_connect_string], timeout=30, retry_on_timeout=True, max_retries=6)

log_level = os.getenv('LOG_LEVEL', 'INFO')

def get_logger(job_id=None):
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    if job_id:
        logger = logging.getLogger(job_id)
    else:
        logger = logging.getLogger('main')
    if len(logger.handlers) == 2:
        return logger

    index = os.getcwd().index('AssociationAbacMiner')
    base_path = os.getcwd()[0:index+20]
    logger.setLevel(log_level)
    logger.addHandler(logging.StreamHandler())

    if job_id:
        hostname = socket.gethostname()
        if not os.path.exists('%s/%s' % (base_path, hostname)):
            os.makedirs('%s/%s' % (base_path, hostname))
        safe_log_filenname = "".join([c for c in job_id if c.isalpha() or c.isdigit() or c == ' ' or c == '_' or c == '-']).rstrip()
        job_log_file_hdlr = logging.FileHandler('%s/%s/%s.log' % (base_path, hostname, safe_log_filenname), mode='w')
        job_log_file_hdlr.setFormatter(formatter)
        logger.addHandler(job_log_file_hdlr)
    # else:
    #     hdlr = logging.FileHandler('%s/executor_output.log' % base_path, mode='a')
    #     hdlr.setFormatter(formatter)
    #     logger.addHandler(hdlr)
    return logger


hour_binning_methods = ['eqf-8', 'eqf-6', 'eqf-5', 'eqf-4', 'eqf-3', 'eqf-2', 'eqw-8', 'eqw-6', 'eqw-4', 'eqw-3', 'eqw-2', 'simple-2', 'simple-3', 'simple-4', 'simple-6', 'simple-8']
parameters = {
    'use_response_elements': [True, False] #whether to use resources and attributes pulled from response elements?
}

