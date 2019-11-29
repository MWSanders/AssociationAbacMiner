import copy
import hashlib
import json
from datetime import datetime

from elasticsearch import helpers

from src.config import config
from src.job import job_utls
from src.model import RuleUtils
from src.model import param_universe_dao
from src.model.ConfigurableEventNormalizer import ConfigurableEventNormalizerNg

client = config.client # MongoClient('mongodb://127.0.0.1:27017', connectTimeoutMS=config.mongodb_timeout)
events_collection = config.events_collection # client.calp.events
es = config.es


class FlatEsUniverseWriter(object):
    def __init__(self, event_enumerator):
        self.event_enumerator = event_enumerator
        self.index_settings = {"settings": { "number_of_shards": 1, "number_of_replicas": 0}}

    def ensure_templates(self):
        flat_template = json.load(open('../resources/flat-possible.json'))
        es.indices.put_template(name='flat-possible', body=flat_template)

    def create_index_only(self, index_name):
        self.ensure_templates()
        es.indices.create(index_name, body=self.index_settings)

    def index_separated_universes(self, index_name):
        print('indexing separated UOR values')
        self.ensure_templates()
        es.indices.delete(index=index_name, ignore=[400, 404])

        print('indexing %s' % index_name)
        es.indices.create(index_name, body=self.index_settings)
        if self.event_enumerator.param_info['possible_params']:
            generator = self.event_enumerator.generate_events()
            helpers.bulk(es, generator, index=index_name, doc_type='doc', refresh=True)
            print('Finished indexing %d records into %s' % (es.count(index=index_name)['count'], index_name))
        else:
            print('Nothing to index for %s, skipping...' % index_name)

class FlatEsLogWriter(object):
    def __init__(self, event_normalizer, all_logs_index='flat-all-log-entries', unique_logs_index='flat-unique-log-entries'):
        self.event_normalizer = event_normalizer
        self.all_logs_index = all_logs_index
        self.unique_logs_index = unique_logs_index
        self.index_settings = {"settings": { "number_of_shards": 1, "number_of_replicas": 0}}

    def ensure_templates(self):
        flat_template = json.load(open('../resources/flat-possible.json'))
        es.indices.put_template(name='flat-possible', body=flat_template)

    def index_log_set(self, query):
        self.ensure_templates()
        print('indexing %s and %s' % (self.all_logs_index, self.unique_logs_index))
        es.indices.delete(index=self.unique_logs_index, ignore=[400, 404])
        es.indices.delete(index=self.all_logs_index, ignore=[400, 404])
        es.indices.create(self.unique_logs_index, body=self.index_settings)
        es.indices.create(self.all_logs_index, body=self.index_settings)

        unique_log_entries = {}
        all_log_entries = []
        for event in events_collection.find(query):
            user_op_resource = self.event_normalizer.normalized_user_op_resource_from_event(event)
            all_log_entries.append(user_op_resource)
            hash_val = hashlib.sha1(json.dumps(user_op_resource, sort_keys=True).encode('utf-8')).hexdigest()
            unique_op_resource = user_op_resource.copy()
            unique_op_resource['_id'] = hash_val
            unique_log_entries[hash_val] = unique_op_resource.copy()
            if len(all_log_entries) > 10000:
                helpers.bulk(es, all_log_entries, index=self.all_logs_index, doc_type='doc', refresh=False)
                all_log_entries.clear()
            if len(unique_log_entries) > 10000:
                helpers.bulk(es, unique_log_entries.values(), index=self.unique_logs_index, doc_type='doc', refresh=False)
                unique_log_entries.clear()
        helpers.bulk(es, all_log_entries, index=self.all_logs_index, doc_type='doc', refresh=True)
        helpers.bulk(es, unique_log_entries.values(), index=self.unique_logs_index, doc_type='doc', refresh=True)


class LogUniverseGenerator(object):
    def __init__(self, event_normalizer, common_scoring_event_normalizer=None, perm_universe_query=None):
        self.event_normalizer = event_normalizer
        self.common_scoring_event_normalizer = common_scoring_event_normalizer
        self.valid_keys = event_normalizer.valid_keys
        self.perm_universe_query = perm_universe_query
        self.valid_keys_user = {'userIdentity_accessKeyId', 'eventTime_weekend','eventTime_weekday', 'eventTime_bin', 'userIdentity_userName'}.intersection(self.valid_keys)
        self.valid_keys_op = self.valid_keys - self.valid_keys_user
        self.service_ops_types = self.load_service_op_types()

    def build_log_universe(self, id, record_limit=0):
        # rtopo_sorted_keys = ['userIdentity_userName','eventVersion','userIdentity_accessKeyId','userIdentity_sessionContext_attributes_mfaAuthenticated','eventType','sourceIPAddress_trunc','sourceIPAddress_bin','eventName_crud_bin', 'eventName_bin','eventSource',
        #                      'eventTime_weekend','sourceIPAddress','userAgent_general_bin','userIdentity_invokedBy','apiVersion','eventName','eventTime_weekday','userAgent_bin','userAgent',
        #                      'requestParameters_encryptionContext_PARAMETER_ARN','requestParameters_path','requestParameters_pipelineName','requestParameters_name','requestParameters_maxResults', 'eventTime_bin']
        rtopo_sorted_keys = ['userIdentity_userName', 'eventVersion', 'userIdentity_accessKeyId', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'eventType', 'sourceIPAddress_trunc', 'sourceIPAddress_bin', 'eventName_crud_bin', 'eventName_bin', 'eventSource',
                             'sourceIPAddress', 'userAgent_general_bin', 'userIdentity_invokedBy', 'apiVersion', 'eventName', 'userAgent_bin', 'userAgent',
                             'requestParameters_dynamodb_tableName', 'requestParameters_codepipeline_name', 'requestParameters_ssm_path', 'requestParameters_kms_encryptionContext_PARAMETER_ARN',
                             'requestParameters_ecs_cluster', 'requestParameters_autoscaling_serviceNamespace', 'requestParameters_ecr_repositoryName', 'requestParameters_ec2_maxResults']
        rtopo_sorted_keys = [e for e in rtopo_sorted_keys if e in self.valid_keys]
        generator_dependent_fields = self.build_dependent_fields(self.event_normalizer.valid_keys)
        possible_params = {}
        param_dependency_mmap = {}
        inv_param_dependency_mmap = {}
        valid_events = 0
        for event in events_collection.find(self.perm_universe_query):
            if record_limit > 0 and valid_events >= record_limit:
                break
            valid_events += 1
            flat_event = self.event_normalizer.normalized_user_op_resource_from_event(event)
            self.process_event(generator_dependent_fields, flat_event, inv_param_dependency_mmap, param_dependency_mmap, possible_params)
            if self.common_scoring_event_normalizer:
                scoring_dependent_fields = self.build_dependent_fields(self.common_scoring_event_normalizer.valid_keys)
                flat_event = self.common_scoring_event_normalizer.normalized_user_op_resource_from_event(event)
                self.process_event(scoring_dependent_fields, flat_event, inv_param_dependency_mmap, param_dependency_mmap, possible_params)
            if valid_events % 50000 == 0:
                print('%d events...' % valid_events)
        # if self.add_binned_field_values:
        #     unbinned_event = {'eventTime_weekday': 'NONE', 'eventTime_weekend':'NONE', 'eventTime_bin':'NONE', 'sourceIPAddress_bin':'NONE', 'sourceIPAddress_trunc':'NONE', 'eventName_bin':'NONE', 'eventName_crud_bin':'NONE', 'userAgent_bin':'NONE', 'userAgent_general_bin':'NONE'}
        #     self.process_event(generator_dependent_fields, unbinned_event, inv_param_dependency_mmap, param_dependency_mmap, possible_params)
        valid_key_sets = {'valid_keys':self.valid_keys, 'valid_keys_user':self.valid_keys_user, 'valid_keys_op':self.valid_keys_op}
        # param_info_objct = {'id': id, 'event_normalizer':self.event_normalizer, 'generator_dependent_fields': generator_dependent_fields, inv_param_dependency_mmap: 'inv_param_dependency_mmap', 'valid_events':valid_events, 'possible_params':possible_params, 'rtopo_sorted_keys':rtopo_sorted_keys,
        #                     'perm_universe_query':self.perm_universe_query, 'valid_key_sets': valid_key_sets}
        # return param_info_objct
        return param_universe_dao.store_universe_info(id, self.event_normalizer, param_dependency_mmap, generator_dependent_fields, inv_param_dependency_mmap, valid_events, possible_params, rtopo_sorted_keys, self.perm_universe_query, valid_key_sets)

    def build_dependent_fields(self, valid_keys):
        # dependent_fields = {'eventName': {'eventSource', 'eventName_bin', 'eventName_crud_bin', ' eventType', 'eventVersion', 'apiVersion'},
        #                     # , 'requestParameters_encryptionContext_PARAMETER_ARN','requestParameters_path','requestParameters_pipelineName','requestParameters_name','requestParameters_maxResults'},
        #                     'eventName_bin': {'eventName_crud_bin'},
        #                     'eventSource': {'eventType', 'eventVersion', 'eventName', 'eventName_bin', 'eventName_crud_bin'},
        #                     'eventTime_weekday': {'eventTime_weekend'},
        #                     'sourceIPAddress': {'sourceIPAddress_bin', 'sourceIPAddress_trunc'},
        #                     'userAgent': {'userAgent_bin', 'userAgent_general_bin', 'eventType', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'sourceIPAddress', 'sourceIPAddress_bin',
        #                                   'userIdentity_invokedBy'},
        #                     'userAgent_bin': {'userAgent_general_bin', 'eventType', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'sourceIPAddress', 'sourceIPAddress_bin', 'userIdentity_invokedBy'},
        #                     'userAgent_general_bin': {'eventType', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'sourceIPAddress', 'sourceIPAddress_bin', 'userIdentity_invokedBy'},
        #                     'userIdentity_invokedBy': {'eventType', 'sourceIPAddress_bin', 'sourceIPAddress'},
        #                     'userIdentity_accessKeyId': {'userIdentity_userName'},
        #                     'apiVersion': {'eventType', 'eventVersion', 'eventSource'},
        #                     'sourceIPAddress_bin': {'eventType', 'sourceIPAddress_trunc'},
        #                     #'eventType': {'userIdentity_sessionContext_attributes_mfaAuthenticated', 'eventVersion'},
        #                     #'requestParameters_encryptionContext_PARAMETER_ARN': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
        #                     #'requestParameters_path': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
        #                     'requestParameters_name': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
        #                     #'requestParameters_maxResults': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
        #                     'requestParameters_pipelineName': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
        #                     'requestParameters_repositoryName': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
        #                     'requestParameters_stackName': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
        #                     'requestParameters_tableName': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
        #                     'requestParameters_pipelineExecutionId': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'}
        #                     }
        dependent_fields = {'eventName': {'eventSource', 'eventName_bin', 'eventName_crud_bin', ' eventType', 'eventVersion', 'apiVersion'},
                            'eventName_bin': {'eventName_crud_bin'},
                            'eventSource': {'eventType', 'eventVersion', 'eventName', 'eventName_bin', 'eventName_crud_bin'},
                            'eventTime_weekday': {'eventTime_weekend'},
                            'sourceIPAddress': {'sourceIPAddress_bin', 'sourceIPAddress_trunc'},
                            'userAgent': {'userAgent_bin', 'userAgent_general_bin', 'eventType', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'sourceIPAddress', 'sourceIPAddress_bin', 'userIdentity_invokedBy'},
                            'userAgent_bin': {'userAgent_general_bin', 'eventType', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'sourceIPAddress', 'sourceIPAddress_bin', 'userIdentity_invokedBy'},
                            'userAgent_general_bin': {'eventType', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'sourceIPAddress', 'sourceIPAddress_bin', 'userIdentity_invokedBy'},
                            'userIdentity_invokedBy': {'eventType', 'sourceIPAddress_bin', 'sourceIPAddress'},
                            'userIdentity_accessKeyId': {'userIdentity_userName'},
                            'apiVersion': {'eventType', 'eventVersion', 'eventSource'},
                            'sourceIPAddress_bin': {'eventType', 'sourceIPAddress_trunc'},
                            'eventType': {'userIdentity_sessionContext_attributes_mfaAuthenticated', 'eventVersion'},
                            'requestParameters_dynamodb_tableName': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
                            'requestParameters_codepipeline_name': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
                            'requestParameters_ssm_path': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
                            'requestParameters_kms_encryptionContext_PARAMETER_ARN': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
                            'requestParameters_ecs_cluster': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
                            'requestParameters_autoscaling_serviceNamespace': {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
                            "requestParameters_ecr_repositoryName": {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'},
                            "requestParameters_ec2_maxResults": {'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'}
                            }
        dependent_fields = copy.deepcopy(dependent_fields)
        keys_to_remove = set()
        for k in dependent_fields.keys():
            if k not in valid_keys: keys_to_remove.add(k)
        for k in keys_to_remove: dependent_fields.pop(k, None)
        return dependent_fields

    def process_event(self, dependent_fields, flat_event, inv_param_dependency_mmap, param_dependency_mmap, possible_params):
        for k, v in flat_event.items():
            RuleUtils.addMulti(possible_params, k, v)
        for k, v in dependent_fields.items():
            for k2 in v:
                if k in flat_event and k2 in flat_event:
                    if k not in param_dependency_mmap:
                        param_dependency_mmap[k] = {}
                    if k2 not in param_dependency_mmap[k]:
                        param_dependency_mmap[k][k2] = {}
                    RuleUtils.addMulti(param_dependency_mmap[k][k2], flat_event[k], flat_event[k2])

                    if k not in inv_param_dependency_mmap:
                        inv_param_dependency_mmap[k] = {}
                    if k2 not in inv_param_dependency_mmap[k]:
                        inv_param_dependency_mmap[k][k2] = {}
                    RuleUtils.addMulti(inv_param_dependency_mmap[k][k2], flat_event[k2], flat_event[k])

    def load_service_op_types(self):
        op_action_types = {}
        for record in config.db.ServiceOpResourceTypes.find():
            id = record['_id']
            record.pop('_id')
            op_action_types[id] = record
        return op_action_types

if __name__ == "__main__":
    start = datetime.utcnow()

    # valid_keys = {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName',
    #               'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path',
    #               'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}
    # fields_common_scoring = ('25fields_common_scoring', valid_keys.copy(), {'eventTime', 'sourceIPAddress', 'eventName', 'userAgent'})
    # fields_common_scoring_ntime = ('25fields_common_scoring_ntime', valid_keys.copy(), {'sourceIPAddress', 'eventName', 'userAgent'})
    # fields_ = ('25fields', valid_keys.copy(), {})
    # fields_eventtime = ('25fields_eventtime', valid_keys.copy(), {'eventTime'})
    # fields_eventtime_sourceipaddress = ('25fields_eventtime_sourceipaddress', valid_keys.copy(), {'eventTime', 'sourceIPAddress'})
    # fields_eventtime_sourceipaddress_eventname = ('25fields_eventtime_sourceipaddress_eventname', valid_keys.copy(), {'eventTime', 'sourceIPAddress', 'eventName'})
    # fields_eventtime_sourceipaddress_eventname_useragent = ('25fields_eventtime_sourceipaddress_eventname_useragent', valid_keys.copy(), {'eventTime', 'sourceIPAddress', 'eventName', 'userAgent'})
    # fields_eventtime_sourceipaddress_eventname_useragent_minus_low_3 = ('25fields_eventtime_sourceipaddress_eventname_useragent_minus_low_3', valid_keys.copy(), {'eventTime', 'sourceIPAddress', 'eventName', 'userAgent'})
    #
    # fields_minus_low_1 =  ('25fields_minus_low1', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion','requestParameters_maxResults'}, set())
    # fields_minus_low_2 =  ('25fields_minus_low2', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path','requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion'}, set())
    # fields_minus_low_3 =  ('25fields_minus_low3', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName'}, set())
    # fields_minus_low_4 =  ('25fields_minus_low4', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name'}, set())
    # fields_minus_low_5 =  ('25fields_minus_low5', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN'}, set())
    # fields_minus_low_6 =  ('25fields_minus_low6', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path'}, set())
    # fields_minus_low_7 =  ('25fields_minus_low7', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId'}, set())
    # fields_minus_low_8 =  ('25fields_minus_low8', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion'}, set())
    # fields_minus_low_9 =  ('25fields_minus_low9', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy'}, set())
    # fields_minus_low_10 = ('25fields_minus_low10', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated'}, set())
    # fields_minus_low_11 = ('25fields_minus_low11', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent'}, set())
    # fields_minus_low_12 = ('25fields_minus_low12', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName'}, set())
    # fields_minus_low_13 = ('25fields_minus_low13', {'sourceIPAddress', 'eventName', 'eventSource'}, set())
    # fields_minus_low_14 = ('25fields_minus_low14', {'sourceIPAddress', 'eventName'}, set())
    # fields_minus_low_15 = ('25fields_minus_low15', {'eventName'}, set())
    #
    # fields_minus_high_1 = ('25fields_minus_high1', {'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_2 = ('25fields_minus_high2', {'eventName', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_3 = ('25fields_minus_high3', {'eventName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_4 = ('25fields_minus_high4', {'eventName', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_5 = ('25fields_minus_high5', {'eventName', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_6 = ('25fields_minus_high6', {'eventName', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_7 = ('25fields_minus_high7', {'eventName', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_8 = ('25fields_minus_high8', {'eventName', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_9 = ('25fields_minus_high9', {'eventName', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_10 = ('25fields_minus_high10', {'eventName', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_11 = ('25fields_minus_high11', {'eventName', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_12 = ('25fields_minus_high12', {'eventName', 'apiVersion', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_13 = ('25fields_minus_high13', {'eventName', 'requestParameters_maxResults', 'eventType'}, set())
    # fields_minus_high_14 = ('25fields_minus_high14', {'eventName', 'eventType'}, set())
    # fields_minus_high_15 = ('25fields_minus_high15', {'eventName'}, set())
    #
    # top4 = ('top4', {'sourceIPAddress', 'eventName', 'eventSource', 'userIdentity_userName', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated'}, set())
    # mid4 = ('mid4', {'eventName', 'userIdentity_userName', 'userIdentity_invokedBy', 'eventVersion', 'userIdentity_accessKeyId', 'requestParameters_path'}, set())
    # bottom4 = ('bottom4', {'eventName', 'userIdentity_userName', 'requestParameters_encryptionContext_PARAMETER_ARN', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion'}, set())
    # top4_12 = ('top4_12', top4[1].union(mid4[1]), set())
    # top4_23 = ('top4_23', mid4[1].union(mid4[1]), set())
    #
    # quarter1 = ('quarter1', {'eventName', 'userIdentity_userName', 'sourceIPAddress', 'eventSource', 'userAgent', 'userIdentity_sessionContext_attributes_mfaAuthenticated'}, set())
    # quarter2 = ('quarter2', {'eventName', 'userIdentity_userName', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'userIdentity_invokedBy', 'eventVersion'}, set())
    # quarter3 = ('quarter3', {'eventName', 'userIdentity_userName', 'userIdentity_accessKeyId', 'requestParameters_path', 'requestParameters_encryptionContext_PARAMETER_ARN'}, set())
    # quarter4 = ('quarter4', {'eventName', 'userIdentity_userName', 'requestParameters_name', 'requestParameters_pipelineName', 'apiVersion', 'requestParameters_maxResults'}, set())
    # top_half = ('top_half', quarter1[1].union(quarter2[1]), set())
    # mid_half = ('mid_half', quarter2[1].union(quarter3[1]), set())
    # bottom_half = ('bottom_half', quarter3[1].union(quarter4[1]), set())
    #
    #
    # fields_useragent = ('25fields_useragent', valid_keys.copy(), {'userAgent'})
    # fields_eventname = ('25fields_eventname', valid_keys.copy(), {'eventName'})
    # fields_sourceipaddress = ('25fields_sourceipaddress', valid_keys.copy(), {'sourceIPAddress'})
    # fields_eventname_useragent = ('25fields_eventname_useragent', valid_keys.copy(), {'eventName', 'userAgent'})
    # fields_sourceipaddress_useragent = ('25fields_sourceipaddress_useragent', valid_keys.copy(), {'sourceIPAddress', 'userAgent'})
    # fields_sourceipaddress_eventname = ('25fields_sourceipaddress_eventname', valid_keys.copy(), {'sourceIPAddress', 'eventName'})
    # fields_sourceipaddress_eventname_useragent = ('25fields_sourceipaddress_eventname_useragent', valid_keys.copy(), {'sourceIPAddress', 'eventName', 'userAgent'})

    # valid_keys = {"eventName","sourceIPAddress","eventSource","userIdentity_userName", "userAgent", "eventVersion", "userIdentity_invokedBy", "userIdentity_sessionContext_attributes_mfaAuthenticated", "requestParameters_repositoryName",
    #           "userIdentity_accessKeyId","requestParameters_encryptionContext_PARAMETER_ARN", "apiVersion", "requestParameters_path", "requestParameters_name"} #, "requestParameters_pipelineName", "requestParameters_tableName"}
    large_valid_keys = {"eventName","sourceIPAddress","eventSource","userIdentity_userName","userAgent","eventVersion","userIdentity_invokedBy","userIdentity_sessionContext_attributes_mfaAuthenticated", "userIdentity_accessKeyId", "apiVersion",
                        "requestParameters_ecr_repositoryName", "requestParameters_dynamodb_tableName","requestParameters_codepipeline_name","requestParameters_ssm_path","requestParameters_kms_encryptionContext_PARAMETER_ARN",
                        "requestParameters_ecs_cluster", "requestParameters_ec2_maxResults", "requestParameters_autoscaling_serviceNamespace", "eventType"}
    fields_large_ps = ('fields_large_ps', large_valid_keys, {'sourceIPAddress', 'eventName', 'userAgent'})
    small_valid_keys = {"eventName", "sourceIPAddress", "eventSource", "userIdentity_userName", "userAgent", "eventVersion", "userIdentity_invokedBy", "userIdentity_sessionContext_attributes_mfaAuthenticated", "userIdentity_accessKeyId", "apiVersion",
                        "requestParameters_ssm_path", "requestParameters_kms_encryptionContext_PARAMETER_ARN", "requestParameters_ecs_cluster", "requestParameters_ec2_maxResults", "requestParameters_autoscaling_serviceNamespace", "eventType"}
    fields_small_ps = ('fields_small_ps', small_valid_keys, {'sourceIPAddress', 'eventName', 'userAgent'})

    for pop_binned_fields in [False]:
        for add_missing_fields in [True]:
            # if not pop_binned_fields and add_missing_fields:
            #     id, valid_keys, fields_to_bin = fields_common_scoring_ntime
            #     id = id + '_amf%s_pbf%s' % (add_missing_fields, pop_binned_fields)
            #     id = id.lower()
            #     print('Generating %s' % id)
            #     event_normalizer = ConfigurableEventNormalizerNg(use_resources=False, bin_method='eqf-6', fields_to_bin=fields_to_bin, valid_keys=valid_keys.copy(), add_missing_fields=add_missing_fields, pop_binned_fields=pop_binned_fields)
            #     common_scoring_event_normalizer = None #ConfigurableEventNormalizerNg(use_resources=False, bin_method='eqf-6', fields_to_bin=set(), valid_keys=valid_keys.copy(), add_missing_fields=add_missing_fields, pop_binned_fields=pop_binned_fields)
            #     log_universe_generator = LogUniverseGenerator(event_normalizer, common_scoring_event_normalizer, job_utls.replace_query_epoch_with_datetime({"eventTime": {"$gte": {"$date": 1506729600000}, "$lte": {"$date": 1513295999000}}}))
            #     param_info_object = log_universe_generator.build_log_universe(id, 9000000)

            for param_universe_tuple in [#fields_minus_high_1, fields_minus_high_2, fields_minus_high_3, fields_minus_high_4, fields_minus_high_5, fields_minus_high_6, fields_minus_high_7, fields_minus_high_8,
                #fields_minus_high_9, fields_minus_high_10, fields_minus_high_11, fields_minus_high_12, fields_minus_high_13, fields_minus_high_14, fields_minus_high_15
                #fields_common_scoring_ntime,
                #fields_, fields_sourceipaddress,  fields_eventname, fields_useragent, fields_eventname_useragent, fields_sourceipaddress_useragent, fields_sourceipaddress_eventname, fields_sourceipaddress_eventname_useragent,
                #fields_minus_low_1, fields_minus_low_2, fields_minus_low_3, fields_minus_low_4, fields_minus_low_5, fields_minus_low_6, fields_minus_low_7, fields_minus_low_8, fields_minus_low_9, fields_minus_low_10, fields_minus_low_10, fields_minus_low_11, fields_minus_low_12, fields_minus_low_13, fields_minus_low_14
                #fields_large_ps,
                fields_small_ps]:
                id, valid_keys, fields_to_bin = param_universe_tuple
                id = id + '_amf%s_pbf%s' % (add_missing_fields, pop_binned_fields)
                id = id.lower()
                print('Generating %s' % id)
                event_normalizer = ConfigurableEventNormalizerNg(use_resources=False, bin_method='eqf-6', fields_to_bin=fields_to_bin, valid_keys=valid_keys.copy(), add_missing_fields=add_missing_fields, pop_binned_fields=pop_binned_fields)
                log_universe_generator = LogUniverseGenerator(event_normalizer, None, job_utls.replace_query_epoch_with_datetime({"eventTime": {"$gte": {"$date": 1490054400000}, "$lte": {"$date": 1513209600000}}}))
                param_info_id = log_universe_generator.build_log_universe(id, 100000000)

    # param_info = param_universe_dao.load_universe_info('56531ac20e9402e22b62fa6064c69957')
    # event_normalizer = ConfigurableEventNormalizerNg.from_param_info(param_info)
    # logging_writer = FlatEsLogWriter(event_normalizer)
    # logging_writer.index_log_set(param_info['perm_universe_query'])

    # original_user_keys = {'userIdentity_accessKeyId', 'eventTime_weekend','eventTime_weekday', 'eventTime_bin', 'userIdentity_userName',}
    # param_info_id = '25fields'
    # param_info = param_universe_dao.load_universe_info(param_info_id)
    # param_info = param_universe_dao.prune_param_info(param_info, param_info['valid_keys_sets']['valid_keys_op'])
    # event_enumerator = EventEnumerator(param_info)
    # es_writer = FlatEsUniverseWriter(event_enumerator)
    # es_writer.index_separated_universes(param_info_id)

    # original_op_keys = {"sourceIPAddress", "userIdentity_sessionContext_attributes_mfaAuthenticated", "eventType", "eventSource", "userAgent_general_bin", "userAgent_bin", "eventName"}
    # event_enumerator = EventEnumerator('56531ac20e9402e22b62fa6064c69957', original_op_keys)
    # es_writer = FlatEsUniverseWriter(event_enumerator)
    # es_writer.index_separated_universes('flat-op-possible')
    time_elapsed = datetime.utcnow() - start
    print('Finished in %ds' % time_elapsed.total_seconds())


