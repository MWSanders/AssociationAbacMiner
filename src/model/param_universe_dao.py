import hashlib
import json

from bson import json_util
from src.model import RuleUtils
from src.config import config
from src.job import job_utls

client = config.client
param_universe_info_collection = config.param_universe_info_collection


def load_universe_info(id, valid_keys_override=None):
    param_info = param_universe_info_collection.find_one({'_id': id})
    param_info = decode_param_info(param_info, valid_keys_override)
    return param_info

def decode_param_info(param_info, valid_keys_override=None):
    for k,v in param_info['dependent_fields'].items():
        param_info['dependent_fields'][k] = set(param_info['dependent_fields'][k])
    for k,v in param_info['inv_param_dependency_mmap'].items():
        for k2, v2 in v.items():
            for k3, v3 in v2.items():
                param_info['inv_param_dependency_mmap'][k][k2][k3] = set(param_info['inv_param_dependency_mmap'][k][k2][k3])
    for k, v in param_info['possible_params'].items():
        param_info['possible_params'][k] = set(param_info['possible_params'][k])
    param_info['event_normalizer_params']['fields_to_bin'] = set(param_info['event_normalizer_params']['fields_to_bin'])
    param_info['event_normalizer_params']['valid_keys'] = set(param_info['event_normalizer_params']['valid_keys'])
    if valid_keys_override:
        prune_param_info(param_info, valid_keys_override)
    param_info['perm_universe_query'] = job_utls.replace_query_epoch_with_datetime(param_info['perm_universe_query'])
    for k in param_info['valid_keys_sets'].keys():
        param_info['valid_keys_sets'][k] = set(param_info['valid_keys_sets'][k])
    return param_info

def prune_param_info(param_info, valid_keys_override):
    # param_info['param_dependency_mmap'] = {k: param_info['param_dependency_mmap'][k] for k in param_info['param_dependency_mmap'] if k in valid_keys_override}
    param_info['inv_param_dependency_mmap'] = {k: param_info['inv_param_dependency_mmap'][k] for k in param_info['inv_param_dependency_mmap'] if k in valid_keys_override}
    param_info['event_normalizer_params']['valid_keys'] = valid_keys_override
    param_info['possible_params'] = {k: param_info['possible_params'][k] for k in param_info['possible_params'] if k in valid_keys_override}
    param_info['rtopo_sorted_keys'] = [k for k in param_info['rtopo_sorted_keys'] if k in valid_keys_override]
    param_info['dependent_fields'] = {k: param_info['dependent_fields'][k] for k in param_info['dependent_fields'] if k in valid_keys_override}
    return param_info

def store_universe_info(id, event_normalizer, dependent_fields, inv_param_dependency_mmap, valid_events, possible_params, rtopo_sorted_keys, perm_universe_query, valid_keys_sets):
    possible_params_record = encode_param_universe(dependent_fields, event_normalizer, id, inv_param_dependency_mmap, perm_universe_query, possible_params, rtopo_sorted_keys, valid_events, valid_keys_sets)
    with open('full_wresources_amftrue_pbftrue.json', 'w') as outfile:
        json.dump(possible_params_record, outfile)
    param_universe_info_collection.replace_one({'_id': possible_params_record['_id']}, possible_params_record, True)
    return possible_params_record['_id']


def universe_period_intersection(event_normalizer, parameter_universe, dynamic_keys):
    current_values = {}
    while True:
        try:
            event = yield
            flat_event = event_normalizer.normalized_user_op_resource_from_event(event)
            for k in dynamic_keys:
                if k in flat_event:
                    RuleUtils.addMulti(current_values, k, flat_event[k])
        except GeneratorExit:
            for k in dynamic_keys:
                parameter_universe['possible_params'][k] = current_values[k]
            for outter_k in parameter_universe['inv_param_dependency_mmap']:
                for inner_k in parameter_universe['inv_param_dependency_mmap'][outter_k]:
                    if outter_k in dynamic_keys:
                        for k,v in parameter_universe['inv_param_dependency_mmap'][outter_k][inner_k].items():
                            parameter_universe['inv_param_dependency_mmap'][outter_k][inner_k][k] = v.intersection(current_values[outter_k])
                    if inner_k in dynamic_keys:
                        keys_to_pop = set() #there's probably a better way to do this with set.symmetric_difference but I'm too tired to try it right now
                        for k in parameter_universe['inv_param_dependency_mmap'][outter_k][inner_k]:
                            if k not in current_values[inner_k]:
                                keys_to_pop.add(k)
                        for k in keys_to_pop:
                            parameter_universe['inv_param_dependency_mmap'][outter_k][inner_k].pop(k)
            return

def encode_existing_param_info(param_info):
    for k, v in param_info['dependent_fields'].items():
        param_info['dependent_fields'][k] = list(param_info['dependent_fields'][k])
    param_info['perm_universe_query'] = json.dumps(param_info['perm_universe_query'], default=json_util.default)
    for k, v in param_info['possible_params'].items():
        param_info['possible_params'][k] = list(param_info['possible_params'][k])
    for k, v in param_info['valid_keys_sets'].items():
        param_info['valid_keys_sets'][k] = list(param_info['valid_keys_sets'][k])
    param_info['event_normalizer_params']['fields_to_bin'] = list(param_info['event_normalizer_params']['fields_to_bin'])
    param_info['event_normalizer_params']['valid_keys'] = list(param_info['event_normalizer_params']['valid_keys'])
    for k, v in param_info['inv_param_dependency_mmap'].items():
        for k2, v2 in v.items():
            for k3, v3 in v2.items():
                param_info['inv_param_dependency_mmap'][k][k2][k3] = list(param_info['inv_param_dependency_mmap'][k][k2][k3])


def encode_param_universe(dependent_fields, event_normalizer, id, inv_param_dependency_mmap, perm_universe_query, possible_params, rtopo_sorted_keys, valid_events, valid_keys_sets):
    for k, v in dependent_fields.items():
        dependent_fields[k] = list(dependent_fields[k])
    for k, v in inv_param_dependency_mmap.items():
        for k2, v2 in v.items():
            for k3, v3 in v2.items():
                inv_param_dependency_mmap[k][k2][k3] = list(inv_param_dependency_mmap[k][k2][k3])
    for k, v in possible_params.items():
        possible_params[k] = list(possible_params[k])
    valid_keys = list(event_normalizer.valid_keys)
    add_missing_fields = event_normalizer.add_missing_fields
    fields_to_bin = list(event_normalizer.fields_to_bin)
    use_resources = event_normalizer.use_resources
    bin_method = event_normalizer.bin_method
    for k in valid_keys_sets.keys():
        valid_keys_sets[k] = list(valid_keys_sets[k])
    event_normalizer_params = {'use_resources': use_resources, 'bin_method': bin_method, 'fields_to_bin': fields_to_bin, 'valid_keys': valid_keys, 'add_missing_fields': add_missing_fields,
                               'pop_binned_fields': event_normalizer.pop_binned_fields}
    possible_params_record = {'inv_param_dependency_mmap': inv_param_dependency_mmap, 'valid_events': valid_events,
                              'event_normalizer_params': event_normalizer_params, 'valid_keys_sets': valid_keys_sets,
                              'possible_params': possible_params, 'perm_universe_query': json.dumps(perm_universe_query, default=json_util.default),
                              'rtopo_sorted_keys': rtopo_sorted_keys, 'dependent_fields': dependent_fields, '_id': hashlib.md5(','.join(s for s in valid_keys).encode('utf-8')).hexdigest()}
    if id:
        possible_params_record['_id'] = id
    return possible_params_record