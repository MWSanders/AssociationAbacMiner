import copy
import itertools
import json
import logging
import math
import os
import sys
from collections import Counter
from multiprocessing import Pool
from src.model import RuleUtils
from src.config import config
import base64, hashlib

from src.config import config
from src.eval.RuleEvaluator import RuleEvaluator

client = config.client #MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
db = client.calp
events_collection = config.events_collection
op_type_collection = config.service_op_resource_collection #db.ServiceOpResourceTypes

class GeneratorNg(object):
    def __init__(self, possible_params, param_dependency_mmap):
        self.param_dependency_mmap = param_dependency_mmap

    def dependencies_allow_normd_event(self, current_user):
        for indep_key, indep_value in self.param_dependency_mmap.items():
            for dep_key, dep_value in indep_value.items():
                if indep_key not in current_user or dep_key not in current_user:
                    continue
                user_ind = current_user[indep_key]
                user_dep = current_user[dep_key]
                allowed_set = dep_value[user_ind]
                if user_dep not in allowed_set:
                    return False
        return True

class UserGeneratorNg(GeneratorNg):
    def __init__(self, possible_params, param_dependency_mmap):
        super().__init__(possible_params, param_dependency_mmap)
        self.possible_params = possible_params
        self.param_dependency_mmap = param_dependency_mmap

    def __next__(self):
        return self.next()

    def next(self):
        for user_name in self.possible_params['user_name']:
            for user_type in self.possible_params['user_type']:
                for op_mfaAuthenticated in self.possible_params['user_op_mfaAuthenticated']:
                    for op_sourceIPAddress in self.possible_params['user_op_sourceIPAddress']:
                        for op_userAgent in self.possible_params['user_op_userAgent']:
                            for op_userAgent_general in self.possible_params['user_op_userAgent_general']:
                                for user_accessKey in self.possible_params['user_accessKey']:
                                    for user_account in self.possible_params['user_account']:
                                        for op_eventType in self.possible_params['user_op_eventType']:
                                            current_user = {
                                                'user_name': user_name,
                                                'user_type': user_type,
                                                'user_op_mfaAuthenticated': op_mfaAuthenticated.lower(),
                                                'user_op_sourceIPAddress': op_sourceIPAddress,
                                                'user_op_userAgent': op_userAgent,
                                                'user_op_userAgent_general': op_userAgent_general,
                                                'user_accessKey': user_accessKey,
                                                'user_account': user_account,
                                                'user_op_eventType': op_eventType
                                            }
                                            if not self.dependencies_allow_normd_event(current_user):
                                                continue
                                            yield current_user
        return

class OpGeneratorNg(GeneratorNg):
    def __init__(self, possible_params, param_dependency_mmap, use_resources=True):
        super().__init__(possible_params, param_dependency_mmap)
        self.possible_params = possible_params
        self.use_resources = use_resources
        self.param_dependency_mmap = param_dependency_mmap

    def __next__(self):
        return self.next()

    def next(self):
        if self.use_resources:
            for op_hour_bucket in self.possible_params['op_hour_bucket']:
                for op_weekday in self.possible_params['op_weekday']:
                    for op_weekend in self.possible_params['op_weekend']:
                            current_op = {
                                'op_hour_bucket': op_hour_bucket,
                                'op_weekday': op_weekday,
                                'op_weekend': op_weekend
                            }
                            if not self.dependencies_allow_normd_event(current_op):
                                continue
                            yield current_op
        else:
            for op_hour_bucket in self.possible_params['op_hour_bucket']:
                for op_weekday in self.possible_params['op_weekday']:
                    for op_weekend in self.possible_params['op_weekend']:
                        for op_eventSource in self.possible_params['op_eventSource']:
                            for op_eventName in self.possible_params['op_eventName']:
                                current_op = {
                                    'op_hour_bucket': op_hour_bucket,
                                    'op_weekday': op_weekday,
                                    'op_weekend': op_weekend,
                                    'op_eventSource': op_eventSource,
                                    'op_eventName': op_eventName
                                }
                                if not self.dependencies_allow_normd_event(current_op):
                                    continue
                                yield current_op
        return


class ResourceGeneratorNg(GeneratorNg):
    def __init__(self, possible_params, param_dependency_mmap, statement=None):
        super().__init__(possible_params, param_dependency_mmap)
        self.statement = statement
        self.possible_params = possible_params
        self.param_dependency_mmap = copy.deepcopy(param_dependency_mmap)
        # self.type_resource_map = type_resource_map.copy()
        self.event_names_counter = Counter()
        # if statement and resource_types:
        #     self.derive_actions_from_resource_types(possible_params, param_dependency_mmap, type_resource_map, resource_types)


    def resources_allow_action(self, action, resource_types):
        resource_sets = self.param_dependency_mmap['resource_op_eventName']['op_resource_types'][action]
        for resource_str in resource_sets:
            if '_' in resource_str:
                required_resources = resource_str.split('_')
                has_all_needed_sub_resources = True
                for required_resource in required_resources:
                    if required_resource not in resource_types:
                        has_all_needed_sub_resources = False
                if has_all_needed_sub_resources: return True
            else:
                if resource_str in resource_types: return True
        return False

    def action_uses_resources(self, action, resource_types):
        resource_sets = self.param_dependency_mmap['resource_op_eventName']['op_resource_types'][action]
        for resource_strs in resource_sets:
            resource_strs = resource_strs.split('_')
            for resource in resource_strs:
                resource = 'resource_' + resource
                if resource in resource_types:
                    return True
        return False

    def derive_actions_from_resource_types(self, possible_params, param_dependency_mmap, type_resource_map, resource_types):
        # Actions that must be present If constraint_map makes an eventName statement, remove other possible actions
        if 'resource_op_eventName' in self.statement['constraints_map']:
            self.possible_params['resource_op_eventName'] = self.statement['constraints_map']['resource_op_eventName']
            if isinstance(self.possible_params['resource_op_eventName'], str): self.possible_params['resource_op_eventName'] = set([self.possible_params['resource_op_eventName']])

        required_types = {}
        for key, value in self.statement['constraints_map'].items():
            if key.startswith('resource_') and not key.startswith('resource_op_'):
                type_map_key = key.replace('resource_', '')
                if isinstance(value, str): value = set([value])
                # if len(value) == 1 and 'None' in value: # If constraint_map makes a resource_type = None statement, remove it from possible resources
                #     self.possible_params.pop(key)
                #     resource_types.discard(type_map_key)
                # else:
                required_types[key] = value # Resource_types that MUST be present.
        if not config.allow_implicit_resources and required_types:
            # remove all actions which do not use the required types
            possible_actions = self.possible_params['resource_op_eventName']
            for action in possible_actions.copy():
                if not self.action_uses_resources(action, required_types.keys()):
                    self.possible_params['resource_op_eventName'].remove(action)

    def __next__(self):
        return self.next()

    def next(self):
        for op_eventSource in self.possible_params['resource_op_eventSource']:
            for op_eventName in self.possible_params['resource_op_eventName']:
                for op_resource_types in self.param_dependency_mmap['resource_op_eventName']['op_resource_types'][op_eventName]:
                    valid_op_type_list = op_resource_types.split('_')
                    if 'None' in valid_op_type_list and len(valid_op_type_list) == 1:
                        current_resource = {
                            'resource_op_eventSource': op_eventSource,
                            'resource_op_eventName': op_eventName,
                            'resources': [{'resource_id': 'None', 'resource_type': 'None'}]
                        }
                        yield current_resource
                        continue
                    resources_list_generator = self.resource_gen_by_types(valid_op_type_list)
                    for resource_list in resources_list_generator:
                        current_resource = {
                            'resource_op_eventSource': op_eventSource,
                            'resource_op_eventName': op_eventName,
                            'resources': resource_list,
                        }
                        if not self.dependencies_allow_normd_event(current_resource):
                            continue
                        resource_types = [x['resource_type'] for x in resource_list]
                        if not self.resources_allow_action(op_eventName, resource_types):
                            continue
                        self.event_names_counter.update([op_eventName])
                        yield current_resource
        return

    def resource_gen_by_types(self, types_list):
        ids_lists = []
        for type_ in types_list:
            type_resource_key = 'resource_' + type_
            if type_resource_key in self.possible_params:
                ids_lists.append(self.possible_params[type_resource_key])
        for resource_permutaiton_tuple in itertools.product(*ids_lists):
            result_resource_map_list = []
            for resource_id in resource_permutaiton_tuple:
                if resource_id == 'None':
                    if 'None' in types_list:
                        yield [{'resource_id': 'None', 'resource_type': 'None'}]
                        continue
                    else:
                        continue
                type_part = resource_id.split(':')[5].split('/')[0]
                resource_map = {'resource_id': resource_id, 'resource_type': type_part}
                result_resource_map_list.append(resource_map)
            if result_resource_map_list:
                yield result_resource_map_list
        return


def userName_from_userIdentity(userIdentity):
    if userIdentity['type'] == 'IAMUser':
        userName = userIdentity['userName']
    else:
        userName = userIdentity['sessionContext']['sessionIssuer']['userName']
    return userName


class UOR_Counter(object):
    def __init__(self, possible_params, param_dependency_mmap, use_resources, statement):
        self.possible_params = possible_params
        self.param_dependency_mmap = param_dependency_mmap
        self.use_resources = use_resources
        self.statement = statement
        u_gen = UserGeneratorNg(self.possible_params, self.param_dependency_mmap).next()
        self.user_gen = []
        for user in u_gen:
            self.user_gen.append(user)
        self.user_count = len(self.user_gen)

        o_gen = OpGeneratorNg(self.possible_params, self.param_dependency_mmap, self.use_resources).next()
        self.op_gen = []
        for op in o_gen:
            self.op_gen.append(op)
        self.op_count = len(self.op_gen)

        self.resource_gen = []
        if use_resources:
            rec_count_gen = ResourceGeneratorNg(self.possible_params, self.param_dependency_mmap, self.statement)
            for resource in rec_count_gen.next():
                self.resource_gen.append(resource)
            self.resource_count = len(self.resource_gen)
        else:
            self.resource_count = 1

        self.total_possible_priv_states = self.user_count * self.op_count * self.resource_count

class ParallelUorHashGenerator(object):
    def __init__(self, possible_params, statement, log_universe_builder, producer_threads=8, use_resources=True):
        self.use_resources = use_resources
        self.producer_threads = producer_threads
        self.statement = statement
        self.service_ops_types = log_universe_builder.service_ops_types
        self.possible_params = self.generate_current_params(possible_params, statement['constraints_map'])
        self.param_dependency_mmap = log_universe_builder.param_dependency_mmap
        self.type_resource_map = log_universe_builder.type_resource_map
        self.event_normalizer = log_universe_builder.event_normalizer

        u_gen = UserGeneratorNg(self.possible_params, self.param_dependency_mmap).next()
        self.user_gen = []
        for user in u_gen:
            self.user_gen.append(user)
        self.user_count = len(self.user_gen)

        o_gen = OpGeneratorNg(self.possible_params, self.param_dependency_mmap, self.use_resources).next()
        self.op_gen = []
        for op in o_gen:
            self.op_gen.append(op)
        self.op_count = len(self.op_gen)

        self.resource_gen = []
        if use_resources:
            rec_count_gen = ResourceGeneratorNg(self.possible_params, self.param_dependency_mmap, self.statement)
            for resource in rec_count_gen.next():
                self.resource_gen.append(resource)
            self.resource_count = len(self.resource_gen)
        else:
            self.resource_count = 1

        self.product_size = self.user_count * self.op_count * self.resource_count

    def generate_current_params(self, possible_params, constraints_map):
        current_params = copy.deepcopy(possible_params)
        statment_constraints = constraints_map
        for key, value in statment_constraints.items():
            if isinstance(value, list):
                current_params[key] = set(x for x in value)
            else:
                tmp_set = set()
                tmp_set.add(value)
                current_params[key] = tmp_set
        return current_params

    def single_threaded_calc_hashest(self, user_gen, op_gen, resource_gen, service_ops_types):
        hashes_list = []
        dicts_list = []
        dicts_list.append(user_gen)
        dicts_list.append(op_gen)
        if self.use_resources:
            dicts_list.append(resource_gen)
        rule_evaluator = RuleEvaluator(service_ops_types)
        for uor_dicts in itertools.product(*dicts_list):
            event = {}
            for x in uor_dicts:
                event.update(x)
            if self.use_resources:
                event = self.event_normalizer.flatten_resources(event)
            event_str = json.dumps(event, sort_keys=True).encode('utf-8')
            if not rule_evaluator.rule_allows_event(event, self.statement['constraints_map']):
                logging.warning('Generated event that does not pass rule used in constructor: %s' % event_str)
            else:
                # event_hash = hashlib.sha1(event_str).hexdigest()
                # event_hash = base64.b64encode(hashlib.sha1(event_str).digest()).decode('utf-8')
                # print(event_str)
                # event_hash = hash(event_str)
                event_hash = int(hashlib.sha256(event_str).hexdigest(), 16)
                hashes_list.append(event_hash)
        return hashes_list

    def calculate_allowed_hashes(self, callback):
        allowed_hashes = []
        if self.producer_threads == 1:
            return self.single_threaded_calc_hashest(self.user_gen, self.op_gen, self.resource_gen, self.service_ops_types)

        if len(self.resource_gen) == 0 and self.use_resources:
            actions_shouldnt_have_resources = True
            for action in self.possible_params['resource_op_eventName']:
                if not 'None' in self.service_ops_types['iam'][action]:
                    actions_shouldnt_have_resources = False
                    break
                self.resource_gen.append({'resource_op_eventName': action,
                                          'resource_op_eventSource': 'iam.amazonaws.com',
                                          'resources': [{'resource_id': 'None', 'resource_type': 'None'}]})
            if not actions_shouldnt_have_resources:
                print("ERROR: No valid resources found")
                sys.exit(2)
            return self.single_threaded_calc_hashest(self.user_gen, self.op_gen, self.resource_gen, self.service_ops_types)

        largest_type = 'users'
        largest_list = self.user_gen
        if len(self.op_gen) > len(self.user_gen):
            largest_type = 'ops'
            largest_list = self.op_gen
        elif len(self.resource_gen) > len(self.user_gen):
            largest_type = 'resources'
            largest_list = self.resource_gen

        chunk_size = math.ceil(len(largest_list) / (8 * self.producer_threads))
        resource_iters = [largest_list[x:x + chunk_size] for x in range(0, len(largest_list), chunk_size)]
        input_data_lists = []
        for data_list in resource_iters:
            if largest_type == 'users':
                input_data_lists.append((data_list, self.op_gen, self.resource_gen, self.service_ops_types))
            elif largest_type == 'ops':
                input_data_lists.append((self.user_gen, data_list, self.resource_gen, self.service_ops_types))
            else:
                input_data_lists.append((self.user_gen, self.op_gen, data_list, self.service_ops_types))

        pool = Pool(self.producer_threads)
        pool.starmap_async(self.single_threaded_calc_hashest, input_data_lists, callback=callback).wait()
        # results = pool.starmap(self.single_threaded_calc_hashest, input_data_lists)
        pool.close()
        pool.join()
        # return results
        # return itertools.chain(allowed_hashes)


if __name__ == "__main__":
    pass
    # query = config.perm_universe_query
    #
    # service_ops_types = LogUniverseBuilder.service_ops_types
    # possible_params = LogUniverseBuilder.possible_params
    # param_dependency_mmap = LogUniverseBuilder.param_dependency_mmap
    # type_resource_map = LogUniverseBuilder.type_resource_map
    #
    # users = ops = resources = 0
    # user_gen = UserGeneratorNg(possible_params, param_dependency_mmap)
    # for user in user_gen.next(): users += 1
    # print('Users: %d' % users)
    # op_gen = OpGeneratorNg(possible_params, param_dependency_mmap)
    # for op in op_gen.next(): ops += 1
    # print('Ops: %d' % ops)
    # resource_gen = ResourceGeneratorNg(possible_params, param_dependency_mmap, type_resource_map)
    # for resource in resource_gen.next(): resources += 1
    # print('Resources: %d' % resources)

