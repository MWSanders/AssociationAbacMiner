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
events_collection = config.events_collection #client.calp_anon.events_resources # #
param_universe_info_collection = config.param_universe_info_collection
es = config.es

r_valid_fields = {'eventType','eventVersion','eventName_crud_bin','userAgent_general_bin','sourceIPAddressLocation','userAgent_bin','userIdentity_userName','eventSource','eventName_bin','eventName','userIdentity_sessionContext_attributes_mfaAuthenticated',
                  'userIdentity_invokedBy','sourceIPAddressInternal','userIdentity_accessKeyId','apiVersion','requestParameters_codepipeline_pipelineName','requestParameters_codepipeline_name','requestParameters_ssm_withDecryption',
                  'requestParameters_cloudformation_stackName','requestParameters_ecr_repositoryName','requestParameters_ecr_registryId','requestParameters_ssm_path','requestParameters_ssm_recursive','requestParameters_kms_encryptionContext_PARAMETER_ARN',
                  'requestParameters_dynamodb_tableName','requestParameters_s3_bucketName','additionalEventData_vpcEndpointId','requestParameters_cloudfront_distributionId','requestParameters_iam_roleName','requestParameters_ec2_maxResults',
                  'requestParameters_cloudfront_maxItems','requestParameters_ecs_taskDefinition','requestParameters_ecs_cluster','requestParameters_logs_limit','requestParameters_monitoring_maxRecords','requestParameters_codebuild_awsActId',
                  'requestParameters_codebuild_payerId','requestParameters_codebuild_userArn','requestParameters_kms_encryptionContext_aws:lambda:FunctionArn','requestParameters_elasticmapreduce_clusterId'}

ip_locations=['internal','IE','CA','QA','US:UNKNOWN','US:AL','US:AK','US:AZ','US:AR','US:CA','US:CO','US:CT','US:DE','US:FL','US:GA','US:HI','US:ID','US:IL','US:IN','US:IA','US:KS','US:KY','US:LA','US:ME','US:MD','US:MA','US:MI','US:MN','US:MS','US:MO','US:MT','US:NE','US:NV','US:NH','US:NJ','US:NM','US:NY','US:NC','US:ND','US:OH','US:OK','US:OR','US:PA','US:RI','US:SC','US:SD','US:TN','US:TX','US:UT','US:VT','US:VA','US:WA','US:WV','US:WI','US:WY','US:DC']


vfscore_fields =  ['eventName','eventSource','userIdentity_userName','sourceIPAddressLocation','userAgent','userAgent_bin','userAgent_general_bin','eventVersion','userIdentity_invokedBy','userIdentity_sessionContext_attributes_mfaAuthenticated','sourceIPAddressInternal','apiVersion','userIdentity_accessKeyId','eventType']
ivfscore_fields = ['eventType','eventVersion','userIdentity_sessionContext_attributes_mfaAuthenticated','userIdentity_invokedBy','userAgent_general_bin','userAgent_bin','userAgent','sourceIPAddressLocation','userIdentity_userName','sourceIPAddressInternal','eventSource','userIdentity_accessKeyId','eventName','apiVersion']
ufscore_fields =  ['eventType','eventVersion','userAgent_general_bin','userAgent_bin','sourceIPAddressLocation','userIdentity_userName','eventSource','userAgent','eventName','userIdentity_sessionContext_attributes_mfaAuthenticated','userIdentity_invokedBy','sourceIPAddressInternal','apiVersion','userIdentity_accessKeyId']
iufscore_fields = ['eventName','userAgent','eventSource','userIdentity_userName','userAgent_bin','userIdentity_accessKeyId','apiVersion','sourceIPAddressLocation','sourceIPAddressInternal','userAgent_general_bin','eventVersion','eventType','userIdentity_sessionContext_attributes_mfaAuthenticated','userIdentity_invokedBy']
valid_fields = set()
valid_fields.update(vfscore_fields)
valid_fields.update(ivfscore_fields)
valid_fields.update(ufscore_fields)
valid_fields.update(iufscore_fields)
common_scoring_fields = valid_fields.copy()
common_scoring_fields.update(['eventName_crud_bin','eventName_bin'])
r_valid_fields.update(valid_fields)

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
        generator = self.event_enumerator.generate_events()
        helpers.bulk(es, generator, index=index_name, doc_type='doc', refresh=True)
        print('Finished indexing %d records into %s' % (es.count(index=index_name)['count'], index_name))

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
        while True:
            try:
                event = yield  # for event in events_collection.find(query):
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
            except GeneratorExit:
                helpers.bulk(es, all_log_entries, index=self.all_logs_index, doc_type='doc', refresh=True)
                helpers.bulk(es, unique_log_entries.values(), index=self.unique_logs_index, doc_type='doc', refresh=True)
                return

class LogUniverseGenerator(object):
    def __init__(self, event_normalizer, perm_universe_query=None, dynamic_keys=set()):
        self.event_normalizer = event_normalizer
        self.valid_keys = event_normalizer.valid_keys
        self.perm_universe_query = perm_universe_query
        self.valid_keys_user = {'userIdentity_accessKeyId', 'eventTime_weekend','eventTime_weekday', 'eventTime_bin', 'userIdentity_userName'}.intersection(self.valid_keys)
        self.valid_keys_op = self.valid_keys - self.valid_keys_user
        self.valid_keys_env = set()
        self.dynamic_keys = dynamic_keys
        for k in self.valid_keys:
            if k.startswith('sourceIPAddress') or k.startswith('userAgent') or k.startswith('userIdentity_invokedBy'):
                self.valid_keys_env.update([k])
            if k.startswith('requestParameters_') or k.startswith('additiona;'):
                self.dynamic_keys.add(k)
        self.dynamic_keys.intersection_update(self.valid_keys)
        self.valid_keys_op = self.valid_keys_op - self.valid_keys_env
        self.service_ops_types = self.load_service_op_types()
        self.rtopo_sorted_keys = ['userIdentity_userName', 'eventVersion', 'userIdentity_accessKeyId', 'userIdentity_sessionContext_attributes_mfaAuthenticated', 'eventType', 'sourceIPAddressLocation', 'sourceIPAddressInternal',
                                  'eventName_crud_bin', 'eventName_bin', 'eventSource', 'sourceIPAddress', 'userAgent_general_bin', 'userIdentity_invokedBy', 'apiVersion', 'eventName', 'userAgent_bin', 'userAgent']
        self.param_universe_info = None

    def coroutine_build_log_universe(self, id, record_limit=0, save_param_universe=False):
        for valid_key in self.event_normalizer.valid_keys:
            if valid_key.startswith('requestParameters_') or valid_key.startswith('additional'):
                self.rtopo_sorted_keys.append(valid_key)
        rtopo_sorted_keys = [e for e in self.rtopo_sorted_keys if e in self.valid_keys]
        generator_dependent_fields = self.build_dependent_fields(self.event_normalizer.valid_keys)
        possible_params = {}
        param_dependency_mmap = {}
        inv_param_dependency_mmap = {}
        valid_events = 0
        while True:
            try:
                event = yield #for event in events_collection.find(self.perm_universe_query):
                if record_limit > 0 and valid_events >= record_limit:
                    break
                valid_events += 1
                flat_event = self.event_normalizer.normalized_user_op_resource_from_event(event)
                self.process_event(generator_dependent_fields, flat_event, inv_param_dependency_mmap, possible_params)
                if valid_events % 50000 == 0:
                    print('%d events...' % valid_events)
            except GeneratorExit:
                # add partial dependencies
                self.add_partial_dependencies(inv_param_dependency_mmap, possible_params)
                valid_key_sets = {'valid_keys':self.valid_keys, 'valid_keys_user':self.valid_keys_user, 'valid_keys_op':self.valid_keys_op, 'valid_keys_env':self.valid_keys_env, 'dynamic_keys': self.dynamic_keys}
                if save_param_universe:
                    param_universe_dao.store_universe_info(id, event_normalizer, generator_dependent_fields, inv_param_dependency_mmap, valid_events, possible_params, self.rtopo_sorted_keys, self.perm_universe_query, valid_key_sets)
                else:
                    self.param_universe_info = param_universe_dao.encode_param_universe(generator_dependent_fields, self.event_normalizer, id, inv_param_dependency_mmap, self.perm_universe_query, possible_params, self.rtopo_sorted_keys, valid_events, valid_key_sets)
                return

    # def universe_period_intersection(self, parameter_universe, dynamic_keys):
    #     current_values = {}
    #     while True:
    #         try:
    #             event = yield
    #             flat_event = self.event_normalizer.normalized_user_op_resource_from_event(event)
    #             for k in dynamic_keys:
    #                 if k in flat_event:
    #                     RuleUtils.addMulti(current_values, k, flat_event[k])
    #         except GeneratorExit:
    #             for k in dynamic_keys:
    #                 parameter_universe['possible_params'][k] = current_values[k]
    #                 # if k in parameter_universe['inv_param_dependency_mmap']:
    #                 #     k2s_to_remove = set()
    #                 #     for k2 in parameter_universe['inv_param_dependency_mmap'][k]:
    #                 #         if k2 not in current_values[k]:
    #                 #             k2s_to_remove.add(k2)
    #                 #     parameter_universe['inv_param_dependency_mmap'][k].difference_update(k2s_to_remove)
    #             for outter_k in parameter_universe['inv_param_dependency_mmap']:
    #                 for inner_k in parameter_universe['inv_param_dependency_mmap'][outter_k]:
    #                     if outter_k in dynamic_keys:
    #                         for k,v in parameter_universe['inv_param_dependency_mmap'][outter_k][inner_k].items():
    #                             parameter_universe['inv_param_dependency_mmap'][outter_k][inner_k][k] = v.intersection(current_values[outter_k])
    #                     if inner_k in dynamic_keys:
    #                         keys_to_pop = set() #there's probably a better way to do this with set.symmetric_difference but I'm too tired to try it right now
    #                         for k in parameter_universe['inv_param_dependency_mmap'][outter_k][inner_k]:
    #                             if k not in current_values[inner_k]:
    #                                 keys_to_pop.add(k)
    #                         for k in keys_to_pop:
    #                             parameter_universe['inv_param_dependency_mmap'][outter_k][inner_k].pop(k)
    #             return

    def build_log_universe(self, id, record_limit=0):
        rtopo_sorted_keys = self.rtopo_sorted_keys.copy
        for valid_key in self.event_normalizer.valid_keys:
            if valid_key.startswith('requestParameters_') or valid_key.startswith('additional'):
                rtopo_sorted_keys.append(valid_key)
        rtopo_sorted_keys = [e for e in rtopo_sorted_keys if e in self.valid_keys]
        dependent_fields = self.build_dependent_fields(self.event_normalizer.valid_keys)
        possible_params = {}
        possible_params['sourceIPAddressLocation'] = set(ip_locations)
        param_dependency_mmap = {}
        inv_param_dependency_mmap = {}
        valid_events = 0
        for event in events_collection.find(self.perm_universe_query):
            if record_limit > 0 and valid_events >= record_limit:
                break
            valid_events += 1
            flat_event = self.event_normalizer.normalized_user_op_resource_from_event(event)
            self.process_event(dependent_fields, flat_event, inv_param_dependency_mmap, possible_params)
            if valid_events % 50000 == 0:
                print('%d events...' % valid_events)
        self.add_partial_dependencies(inv_param_dependency_mmap, possible_params)
        valid_key_sets = {'valid_keys':self.valid_keys, 'valid_keys_user':self.valid_keys_user, 'valid_keys_op':self.valid_keys_op, 'valid_keys_env':self.valid_keys_env}
        return param_universe_dao.store_universe_info(id, self.event_normalizer, dependent_fields, inv_param_dependency_mmap, valid_events, possible_params, rtopo_sorted_keys, self.perm_universe_query, valid_key_sets)

    def add_partial_dependencies(self, inv_param_dependency_mmap, possible_params):
        if 'sourceIPAddressLocation' in self.valid_keys:
            possible_params['sourceIPAddressLocation'] = set(ip_locations)
        # add partial dependencies
        if 'userIdentity_invokedBy' in self.valid_keys and 'sourceIPAddressLocation' in self.valid_keys:
            for location in ip_locations:
                if location == 'internal': continue
                inv_param_dependency_mmap['userIdentity_invokedBy']['sourceIPAddressLocation'][location] = {"NONE", "signin__amazonaws__com"}
            inv_param_dependency_mmap['userIdentity_invokedBy']['sourceIPAddressLocation']['internal'] = {"NONE", "signin__amazonaws__com", "internal"}
            inv_param_dependency_mmap['sourceIPAddressLocation']['userIdentity_invokedBy']['NONE'] = set(ip_locations)
            inv_param_dependency_mmap['sourceIPAddressLocation']['userIdentity_invokedBy']['signin__amazonaws__com'] = set(ip_locations)
        # if 'userIdentity_invokedBy' in self.valid_keys and 'sourceIPAddressBin' in self.valid_keys:
        #     for location in ip_locations:
        #         inv_param_dependency_mmap['userIdentity_invokedBy']['sourceIPAddressBin'][location] = {"NONE", "signin__amazonaws__com"}
        #     inv_param_dependency_mmap['sourceIPAddressBin']['userIdentity_invokedBy']['NONE'] = ip_locations
        #     inv_param_dependency_mmap['sourceIPAddressBin']['userIdentity_invokedBy']['signin__amazonaws__com'] = ip_locations
        if 'sourceIPAddressLocation' in self.valid_keys and 'sourceIPAddressInternal' in self.valid_keys:
            inv_param_dependency_mmap['sourceIPAddressLocation']['sourceIPAddressInternal']['NONE'] = set(ip_locations)
            for location in ip_locations:
                if location == 'internal': continue
                inv_param_dependency_mmap['sourceIPAddressInternal']['sourceIPAddressLocation'][location] = {"NONE"}

    def build_dependent_fields(self, valid_keys):
        dependent_field_lists = [['eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin', 'eventType', 'eventVersion', 'apiVersion'],
                                 ['eventType', 'userIdentity_sessionContext_attributes_mfaAuthenticated'],
                                 ['userIdentity_userName', 'userIdentity_accessKeyId'],
                                 ['sourceIPAddressLocation', 'sourceIPAddressInternal', 'userIdentity_invokedBy'],
                                 ['userAgent', 'userAgent_bin', 'userAgent_general_bin', 'userIdentity_invokedBy']]
        for valid_key in valid_keys:
            if valid_key.startswith('requestParameters_') or valid_key.startswith('additional'):
                dependent_field_lists.append([valid_key, 'eventName', 'eventSource', 'eventName_bin', 'eventName_crud_bin'])
        dependent_fields = {}
        for v in dependent_field_lists:
            for field_a in v:
                for field_b in v:
                    if field_a == field_b: continue
                    RuleUtils.addMulti(dependent_fields, field_a, field_b)
        keys_to_remove = set()
        for k in dependent_fields.keys():
            if k not in valid_keys: keys_to_remove.add(k)
        for k in keys_to_remove: dependent_fields.pop(k, None)
        return dependent_fields

    def process_event(self, dependent_fields, flat_event, inv_param_dependency_mmap, possible_params):
        for k, v in flat_event.items():
            RuleUtils.addMulti(possible_params, k, v)
        for k, v in dependent_fields.items():
            for k2 in v:
                if k in flat_event and k2 in flat_event:

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

    # vf_score_tup = ('vf_score_fields_nd', vfscore_fields, {'userAgent'}, set())
    # ivfscore_fields = ('ivfscore_fields', ivfscore_fields, {'userAgent'})
    # ufscore_fields = ('ufscore_fields', ufscore_fields, {'userAgent'})
    # iufscore_fields = ('iufscore_fields', iufscore_fields, {'userAgent'})

    # tuple_seeds = []
    # tuple_seeds.append(vf_score_tup)
    # tuple_seeds.append(ivfscore_fields)
    # tuple_seeds.append(ufscore_fields)
    # tuple_seeds.append(iufscore_fields)
    field_subsets = []
    #field_subsets.append(('full_wresources', r_valid_fields, {'eventName', 'userAgent'}, set([s for s in r_valid_fields if s.startswith('requestParameters') or s.startswith('additionalEventData')])))
    # field_subsets.append(('short_wresources_abac', r_valid_fields, {'eventName', 'userAgent'}, set([s for s in r_valid_fields if s.startswith('requestParameters') or s.startswith('additionalEventData')])))
    #field_subsets.append(('short_wresources_rbac', r_valid_fields, {'eventName', 'userAgent'}, set([s for s in r_valid_fields if s.startswith('requestParameters') or s.startswith('additionalEventData')])))

    # field_subsets.append(('common_scoring_nd', common_scoring_fields, {'eventName', 'userAgent'}, set()))
    #field_subsets.append(('short_nd_rbac', {'eventName', 'userIdentity_userName'}, set(), set()))
    #field_subsets.append(('full_wresources', r_valid_fields, {'eventName', 'userAgent'}, set([s for s in r_valid_fields if s.startswith('requestParameters') or s.startswith('additionalEventData')])))
    # field_subsets.append(('full_nd_abac', r_valid_fields, {'eventName', 'userAgent'}, set()))
    field_subsets.append(('full_nd_15', common_scoring_fields, {'eventName', 'userAgent'}, set()))
    field_subsets.append(('short_40_nd', r_valid_fields, {'eventName', 'userAgent'}, set([s for s in r_valid_fields if s.startswith('requestParameters') or s.startswith('additionalEventData')])))
    #field_subsets.append(('full_common_scoring_nd', common_scoring_fields, {'eventName', 'userAgent'}, set()))
    # field_subsets.append(('fs_b_userAgent_nd', valid_fields, {'userAgent'}, set()))
    # field_subsets.append(('full_rbac_nd', {'eventName', 'userIdentity_userName'}, {}, set()))
    # for tup_name, tup_fields, tup_bins, tup_dyn_keys in tuple_seeds:
    #     tmp_vsscore_valid_fields = tup_fields.copy()
    #     for i in range(0, len(tmp_vsscore_valid_fields)-1):
    #         current_field = tmp_vsscore_valid_fields.pop()
    #         field_set = set(tmp_vsscore_valid_fields)
    #         if i < 10:
    #             current_subset = (('%s_minus_lowest0%d' % (tup_name, i)), field_set, tup_bins, tup_dyn_keys)
    #         else:
    #             current_subset = (('%s_minus_lowest%d' % (tup_name, i)), field_set, tup_bins, tup_dyn_keys)
    #         field_subsets.append(current_subset)
    #
    #     tmp_vsscore_valid_fields = tup_fields.copy()
    #     for i in range(0, len(tmp_vsscore_valid_fields)-1):
    #         current_field = tmp_vsscore_valid_fields.pop(0)
    #         field_set = set(tmp_vsscore_valid_fields)
    #         if i < 10:
    #             current_subset = (('%s_minus_highest0%d' % (tup_name, i)), field_set, tup_bins, tup_dyn_keys)
    #         else:
    #             current_subset = (('%s_minus_highest%d' % (tup_name, i)), field_set, tup_bins, tup_dyn_keys)
    #         field_subsets.append(current_subset)
    # for id, keys, bins, dyn_keys in tuple_seeds:
    #     field_subsets.append((id, set(keys), bins, dyn_keys))
    # field_subsets.append(('full_eventName_nd', {'eventName'}, {}, set()))
    # field_subsets.append(('eventName_nd', {'eventName'}, {}, set()))

    for pop_binned_fields in [True]:
        for add_missing_fields in [True]:
            for param_universe_tuple in field_subsets: #vsscore_field_subsets:
                id, valid_keys, fields_to_bin, dynamic_keys = param_universe_tuple
                perm_universe_query = job_utls.replace_query_epoch_with_datetime({"eventTime": {"$gte": {"$date": 1490054400000}, "$lte": {"$date": 1500076799000}}})
                if 'full' in id:
                    perm_universe_query = job_utls.replace_query_epoch_with_datetime({"eventTime": {"$gte": {"$date": 1490054400000}, "$lte": {"$date": 1531439999000}}})
                id = id + '_amf%s_pbf%s' % (add_missing_fields, pop_binned_fields)
                id = id.lower()
                if param_universe_info_collection.find({'_id': id}).count() > 0:
                    print('%s already exists in abac_param_universe_info, skipping...' % id)
                else:
                    print('Generating %s' % id)
                    event_normalizer = ConfigurableEventNormalizerNg(use_resources=False, bin_method='eqf-6', fields_to_bin=fields_to_bin, valid_keys=valid_keys.copy(), add_missing_fields=add_missing_fields, pop_binned_fields=pop_binned_fields)
                    log_universe_generator = LogUniverseGenerator(event_normalizer, perm_universe_query, dynamic_keys)
                    build_param_info_pipe = log_universe_generator.coroutine_build_log_universe(id, 0, True)
                    build_param_info_pipe.__next__()
                    for event in events_collection.find(perm_universe_query):
                        build_param_info_pipe.send(event)
                    build_param_info_pipe.close()

    time_elapsed = datetime.utcnow() - start
    print('Finished in %ds' % time_elapsed.total_seconds())

