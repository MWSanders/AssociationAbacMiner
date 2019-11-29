import json
import traceback
import sys
from src.model import RuleUtils
from src.eval.WscCalculator import WscCalculator
from src.model.RuleEval import RuleEval
from src.config import config
import logging
from elasticsearch import Elasticsearch

es = Elasticsearch([config.es_connect_string])

class EnvOverassignmentCalculator(object):
    def __init__(self, user_possible_privs, op_possible_privs, resource_possible_privs, service_action_types, job, generation_param_info, scoring_param_info, unique_logs_index, all_logs_index):
        self.job_config = job['config']
        self.generation_param_info = generation_param_info
        self.scoring_param_info = scoring_param_info
        self.op_possible_index = 'flat-op-' + self.scoring_param_info['_id'].lower()
        self.user_possible_index = 'flat-user-' + self.scoring_param_info['_id'].lower()
        self.environment_possible_index = 'flat-env-' + self.scoring_param_info['_id'].lower()
        self.unique_logs_index = unique_logs_index #'flat-unique-logs-' + self.generation_param_info['_id'].lower()
        self.all_logs_index = all_logs_index #'flat-all-logs-' + self.generation_param_info['_id'].lower()
        # self.logger = logging.getLogger(job['_id'])
        self.user_possible_privs = user_possible_privs
        self.op_possible_privs = op_possible_privs
        self.resource_possible_privs = resource_possible_privs
        self.total_possible_priv_states = user_possible_privs * op_possible_privs * resource_possible_privs
        print('Total Privs: %d uPrivs: %d oPrivs: %d rPrivs: %d' % (self.total_possible_priv_states, user_possible_privs, op_possible_privs, resource_possible_privs))
        self.service_action_types = service_action_types
        self.wsc_calc = WscCalculator(self.scoring_param_info)


    def process_type_restrictions(self, op_constraints, resource_constraints):
        invalid_types = set()
        invalid_events = set()
        #TODO process op_constraints
        for constraint in resource_constraints:
            key = constraint.split('=')[0].split('_')[1]
            value = constraint.split('=')[1]
            if value == 'None':
                invalid_types.add(key)
        for service_key, service_map in self.service_action_types.items():
            for action_key, types_list in service_map.items():
                if action_key == '_id':
                    continue
                types_set = set()
                for type in types_list:
                    types_set.update(type.split('_'))
                if set.intersection(types_set, invalid_types):
                    invalid_events.add(action_key)
        return invalid_types, invalid_events


    def replace_key(self, constraint, new_key):
        value = constraint.split('=')[1]
        return '%s=%s' % (new_key, value)


    def generate_queries(self, rule_constrains_names):
        queries = []
        # separated_rule_constrains_names = self.convert_event_constraints_to_separated(rule_constrains_names)
        user_constraints = set()
        op_constraints = set()
        env_constraints = set()
        user_possible = op_possible = resource_possible = 0
        for constraint in rule_constrains_names:
            key = constraint.split('=')[0]
            if key.startswith('sourceIPAddress') or key.startswith('userAgent') or key.startswith('userIdentity_invokedBy'):
                env_constraints.add(constraint)
            elif key in self.generation_param_info['valid_keys_sets']['valid_keys_user']:
                user_constraints.add(constraint)
            elif key in self.generation_param_info['valid_keys_sets']['valid_keys_op']:
                op_constraints.add(constraint)

        invalid_events = set()
        # invalid_types, invalid_events = self.process_type_restrictions(op_constraints, resource_constraints)
        # users
        req_head = {'index': self.user_possible_index, 'type': 'doc'}
        if user_constraints:
            user_query = RuleUtils.create_terms_filter_from_constraints(user_constraints)
        else:
            user_query = {"query" : {"match_all" : {}}, "size" : 0}
        queries.extend([req_head, user_query])
        # ops
        req_head = {'index': self.op_possible_index, 'type': 'doc'}
        if op_constraints:
            op_query = RuleUtils.op_create_terms_filter_from_constraints(op_constraints, invalid_events)
        else:
            op_query = {"query": {"match_all": {}}, "size": 0}
        queries.extend([req_head, op_query])
        # envs
        req_head = {'index': self.environment_possible_index, 'type': 'doc'}
        if env_constraints or invalid_events:
            resource_query = RuleUtils.op_create_terms_filter_from_constraints(env_constraints, invalid_events)
        else:
            resource_query = {"query": {"match_all": {}}, "size": 0}
        queries.extend([req_head, resource_query])

        # unique log entries
        query = RuleUtils.create_query_convert_resource_to_type(rule_constrains_names)
        req_head = {'index': self.unique_logs_index, 'type': 'doc'}
        queries.extend([req_head, query])
        # all log entries
        req_head = {'index': self.all_logs_index, 'type': 'doc'}
        queries.extend([req_head, query])
        return queries

    def process_separated_buffered_results(self, rules, responses):
        invalid_rules = 0
        for i in range(0, len(rules)):
            try:
                current_rule = rules[i]
                current_rule.allowed_users = responses[i*5]['hits']['total'] if responses[i * 5]['hits']['total'] > 0 else 1
                current_rule.allowed_ops = responses[i*5+1]['hits']['total'] if responses[i * 5+1]['hits']['total'] > 0 else 1
                current_rule.allowed_resources = responses[i*5+2]['hits']['total'] if responses[i * 5+2]['hits']['total'] > 0 else 1
                current_rule.allowed_events_count = current_rule.allowed_users * current_rule.allowed_ops * current_rule.allowed_resources
                current_rule.unique_log_entries = responses[i*5+3]['hits']['total']
                current_rule.all_log_entries = responses[i*5+4]['hits']['total']
                current_rule.scores['wsc'] = len(current_rule.constraints) #self.wsc_calc.compute_rule_wsc(current_rule)
            except KeyError as e:
                traceback.print_exc()
                print('RESPONSE INDEX: %d' % i)
                print('RESPONSE0: %s' % responses[i*5])
                print('RESPONSE1: %s' % responses[i * 5+1])
                print('RESPONSE2: %s' % responses[i * 5+2])
                print('RESPONSE3: %s' % responses[i * 5+3])
                print('RESPONSE4: %s' % responses[i * 5+4])
                raise

            current_rule.overassignment_total = current_rule.allowed_events_count - current_rule.unique_log_entries
            current_rule.overassignment_rate = current_rule.overassignment_total / self.total_possible_priv_states
            if (current_rule.overassignment_rate < 0.0) or current_rule.unique_log_entries == 0: # and current_rule.unique_log_entries != current_rule.allowed_events_count)
                print('invalid rule found: %s' % json.dumps(current_rule.q).replace("\'", "\""))
                print('Overassignment_rate: %f, current_rule.allowed_users: %d, current_rule.allowed_ops: %d, allowed_events_count: %d, unique_log_entries: %d' % (current_rule.allowed_users, current_rule.allowed_ops, current_rule.overassignment_rate, current_rule.allowed_events_count, current_rule.unique_log_entries))
                invalid_rules += 1
                # sys.exit(5)
            # if self.job_config['abac_params']['metric']['max_threshold'] > 0 and current_rule.overassignment_total > self.job_config['abac_params']['metric']['max_threshold']:
            #     current_rule.sort_metric_value = float('-inf')
            #     continue
            beta = self.job_config['abac_params']['metric']['beta'] #B<1 overR is more important, B>1 covR is more important
            # gamma = self.job_config['abac_params']['metric']['gamma']
            overR = 1 - current_rule.overassignment_rate
            covR = current_rule.coverage_rate
            current_rule.scores['harmonic_mean'] = (1+(beta*beta)) * ((overR*covR) / (((beta*beta)*overR)+covR))
            # current_rule.scores['harmonic_mean_wsc'] = (gamma * (1/current_rule.scores['wsc'])) * current_rule.scores['harmonic_mean']
            current_rule.scores['arithmetic_mean'] = ((beta * covR) + (overR))/2
            covRW = current_rule.all_log_entries / current_rule.scores['wsc']
            current_rule.scores['arithmetic_mean_wsc'] = ((beta * covRW) + (overR)) / 2
            # current_rule.scores['arithmetic_mean_wsc'] = (gamma * (1/current_rule.scores['wsc'])) * current_rule.scores['arithmetic_mean']
            over_total = current_rule.overassignment_total if current_rule.overassignment_total > 0 else 1
            current_rule.scores['product'] = (current_rule.unique_log_entries * beta) * (1/over_total)
            current_rule.scores['Qrul_count1'] = (current_rule.unique_log_entries) * (1-((beta * current_rule.overassignment_total)/current_rule.allowed_events_count))
            current_rule.scores['Qrul_count1_i'] = (current_rule.unique_log_entries) * (1 - beta * current_rule.overassignment_total / current_rule.unique_log_entries)
            current_rule.scores['Qrul_freq1'] = (current_rule.all_log_entries) * (1 - ((beta * current_rule.overassignment_total) / current_rule.allowed_events_count))
            current_rule.scores['Qrul_freq1_i'] = (current_rule.all_log_entries) * (1 - beta * current_rule.overassignment_total / current_rule.all_log_entries)
            current_rule.scores['l_dist'] = current_rule.under_assignments + (beta * current_rule.overassignment_total)
            # current_rule.scores['product_wsc'] = (gamma * (1/current_rule.scores['wsc'])) * current_rule.scores['product']
            # user all_log_entries for |[p] ∩ UP| (UP tubples covered by rule and not already covered by polity) because if a entry was already covered by rule it would have been removed from the log set for search & scoring
            current_rule.scores['Qrul_count'] = (current_rule.unique_log_entries / current_rule.scores['wsc']) * (1-((float(beta) * current_rule.overassignment_total)/current_rule.allowed_events_count))
            current_rule.scores['Qrul_count_i'] = (current_rule.unique_log_entries / current_rule.scores['wsc']) * (1 - beta * current_rule.overassignment_total / current_rule.unique_log_entries)
            current_rule.scores['Qrul_freq'] = (current_rule.all_log_entries / current_rule.scores['wsc']) * (1-((beta * current_rule.overassignment_total)/current_rule.allowed_events_count))
            current_rule.scores['Qrul_freq_i'] = (current_rule.all_log_entries / current_rule.scores['wsc']) * (1 - beta * current_rule.overassignment_total / current_rule.all_log_entries)
            current_rule.sort_metric_value = current_rule.scores[self.job_config['abac_params']['metric']['type']]
        if invalid_rules > 0:
            print('WARNING: %d/%d tested rules were invalid' % (invalid_rules, len(rules)))
        return rules

    def calc_overassignments(self, itemsets, mapping, names, coverage_rate_denominator, single_value_columns):
        requests = []
        rules = []
        results = []
        for item, count in itemsets.items():
            rule_constrains_names = RuleUtils.item_to_constraint_set(item, mapping, names)
            rule_constrains_names.update(single_value_columns)
            item_queries = self.generate_queries(rule_constrains_names)
            requests.extend(item_queries)

            rule_eval = RuleEval()
            rule_eval.set_constrants(rule_constrains_names)
            rule_eval.all_log_entries = count
            rule_eval.coverage_rate = count / float(coverage_rate_denominator)
            rule_eval.under_assignments = coverage_rate_denominator - count
            rule_eval.q = item_queries[7]
            rules.append(rule_eval)

            if len(requests) > 400:
                resp = es.msearch(body=requests, request_timeout=300)
                requests.clear()
                results.extend(self.process_separated_buffered_results(rules, resp['responses']))
                rules.clear()
                if len(results) > 2000:
                    reverse = True
                    if self.job_config['abac_params']['metric']['type'] == 'l_dist':
                        reverse = False
                    results = sorted(results, key=lambda x: x.sort_metric_value, reverse=reverse)
                    results = results[:500]  # only keep top 1000 to save memory
        if requests:
            resp = es.msearch(body=requests, request_timeout=300)
            results.extend(self.process_separated_buffered_results(rules, resp['responses']))
            requests.clear()
        return results


if __name__ == "__main__":
    # print(service_action_types)
    pass