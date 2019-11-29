import itertools
import math
from multiprocessing import Pool
# from multiprocessing.pool import ThreadPool as Pool

from bson import json_util
from orangecontrib.associate.fpgrowth import *
from src.model.RuleEval import RuleEval
import time
import os
from src import TableCreator
from src.eval.EnvOverassignmentCalculator import EnvOverassignmentCalculator
from src.model.EnvLogUniverseGenerator import *
from src.model.EventEnumerator import EventEnumerator
from src.eval.RuleMerger import RuleMerger
from src.config import config
from src.model.EnvParitionedUniverseIndexer import EnvPartitionedUniverseIndexer

client = config.client
events_collection = config.events_collection
policy_collection = config.policy_colleciton
jobs_collection = config.jobs_collection
service_op_resource_collection = config.service_op_resource_collection

class EnvFlatAbacRuleMiner(object):
    def __init__(self, job):
        self.job = job
        self.generation_param_info_id = job['config']['abac_params']['generation_param_info_id']
        self.generation_param_info = param_universe_dao.load_universe_info(self.generation_param_info_id)
        self.scoring_param_info_id = job['config']['abac_params']['generation_param_info_id'] #job['config']['scoring_param_info_id']
        self.scoring_param_info = param_universe_dao.load_universe_info(self.scoring_param_info_id)
        self.scoring_event_normalizer = ConfigurableEventNormalizerNg.from_param_info(self.scoring_param_info)
        self.op_possible_index = 'flat-op-' + self.scoring_param_info_id
        self.user_possible_index = 'flat-user-' + self.scoring_param_info_id
        self.environment_possible_index = 'flat-env-' + self.scoring_param_info_id
        index_safe_name = self.generation_param_info_id + '-' + self.job['_id']
        index_safe_name = "".join([c for c in index_safe_name if (c.isalpha() or c.isdigit()) or c in ['_', '-', '+']]).rstrip().lower()[:254]
        self.unique_logs_index = 'flat-unique-logs-' + index_safe_name
        self.all_logs_index = 'flat-all-logs-' + index_safe_name
        # self.logger = config.get_logger(job['_id'])
        self.generation_event_normalizer = ConfigurableEventNormalizerNg.from_param_info(self.generation_param_info)
        self.es = config.es
        self.user_possible_privs = None
        self.op_possible_privs = None
        self.env_possible_privs = None
        self.service_action_types = None
        self.obs_query = job['obs_query']
        self.perm_universe_query = job['obs_opr_query']
        self.total_possible_priv_states = None
        if os.cpu_count() == 20:
            self.worker_threads = 3
        else:
            self.worker_threads = 3 #os.cpu_count()
        self.key_filter = set(self.scoring_param_info['event_normalizer_params']['valid_keys'])#job['config']['abac_params']['key_filter']
        self.index()

    def index(self):
        # Index all and unique log events that occurred during the OBS period
        print('Indexing obs period logs')
        logging_writer = FlatEsLogWriter(self.generation_event_normalizer, self.all_logs_index, self.unique_logs_index)
        index_logs_pipe = logging_writer.index_log_set(self.job['obs_query'])
        index_logs_pipe.__next__()
        prune_params_to_current = param_universe_dao.universe_period_intersection(self.scoring_event_normalizer, self.generation_param_info, self.generation_param_info['valid_keys_sets']['dynamic_keys'])
        prune_params_to_current.__next__()
        # build_param_info_pipe = log_generator.coroutine_build_log_universe(self.generation_param_info_id)
        # build_param_info_pipe.__next__()
        for event in events_collection.find(self.job['obs_query']):
            index_logs_pipe.send(event)
            prune_params_to_current.send(event)
            # build_param_info_pipe.send(event)
        index_logs_pipe.close()
        prune_params_to_current.close()

        #Index param universe
        env_universe_indexer = EnvPartitionedUniverseIndexer(self.scoring_param_info_id)
        env_universe_indexer.index()
        self.validate_possible_privs(env_universe_indexer)

    def validate_possible_privs(self, env_universe_indexer):
        self.total_possible_priv_states, self.user_possible_privs, self.op_possible_privs, self.env_possible_privs = env_universe_indexer.count_correct_privs()
        while True:
            total, user, op, env = env_universe_indexer.count_indexed_privs()
            if total == self.total_possible_priv_states and user == self.user_possible_privs and op == self.op_possible_privs:
                print('Found correct number of privs indexed, continuing...')
                break
            print('Incorrect number of privs indexed, sleeping...')
            print('Total: %d/%d User: %d/%d Op %d/%d' % (total, self.total_possible_priv_states, user, self.user_possible_privs, op, self.op_possible_privs))
            time.sleep(5)

    def load_service_action_types(self):
        op_type_collection = config.service_op_resource_collection
        self.service_action_types = {}
        for service_map in op_type_collection.find():
            id = service_map['_id']
            self.service_action_types[id] = {}
            self.service_action_types[id].update(service_map)
        return self.service_action_types

    def chunks(self, data, SIZE=10000):
        if SIZE <= 0:
            print('Chunck size was 0, changing to 1')
            SIZE = 1
        it = iter(data)
        for i in range(0, len(data), SIZE):
            yield {k: data[k] for k in itertools.islice(it, SIZE)}

    def get_best_result(self, result_lists):
        best_score = float('-inf')
        if self.job['config']['abac_params']['metric']['type'] == 'l_dist':
            best_score = float('inf')
        best_rule = None
        for result in result_lists:
            for rule in result:
                if self.job['config']['abac_params']['metric']['type'] == 'l_dist' and rule.sort_metric_value < best_score:
                    best_score = rule.sort_metric_value
                    best_rule = rule
                elif self.job['config']['abac_params']['metric']['type'] != 'l_dist' and rule.sort_metric_value > best_score:
                    best_score = rule.sort_metric_value
                    best_rule = rule
                if best_rule and rule and rule.sort_metric_value == best_score and len(rule.constraints) < len(best_rule.constraints):
                    best_rule = rule
        return best_rule

    def rule_from_set(self, single_value_columns):
        rule_eval = RuleEval()
        rule_eval.set_constrants(single_value_columns)
        rule_eval.all_log_entries = 0
        rule_eval.coverage_rate = 0
        rule_eval.overassignment_rate_all = 0
        rule_eval.overassignment_total = 0
        return rule_eval

    def mine_rules_for_window(self):
        already_covered_rules = []
        main_start = datetime.utcnow()
        oa_calc = EnvOverassignmentCalculator(user_possible_privs=self.user_possible_privs, op_possible_privs=self.op_possible_privs,
                                           resource_possible_privs=self.env_possible_privs, service_action_types=self.service_action_types,
                                           job=self.job, generation_param_info=self.generation_param_info, scoring_param_info = self.scoring_param_info,
                                           unique_logs_index=self.unique_logs_index, all_logs_index=self.all_logs_index)
        pool = Pool(self.worker_threads)
        current_rule = 0
        original_all_logs_count = self.es.count(index=self.all_logs_index, doc_type='doc', body={"query": {"match_all": {}}})['count']
        original_unique_logs_count = self.es.count(index=self.unique_logs_index, doc_type='doc', body={"query": {"match_all": {}}})['count']
        itemset_frequency = self.job['config']['abac_params']['itemset_freq']
        itemset_limit = self.job['config']['abac_params']['itemset_limit']
        while True:
            current_rule += 1
            uncovered_all_logs_count = self.es.count(index=self.all_logs_index, doc_type='doc', body={"query": {"match_all": {}}})['count']
            uncovered_unique_logs_count = self.es.count(index=self.unique_logs_index, doc_type='doc', body={"query": {"match_all": {}}})['count']
            print('%d rules, %d uncovered logs %d unique uncovered log entries' % (len(already_covered_rules), uncovered_all_logs_count, uncovered_unique_logs_count))
            if uncovered_unique_logs_count == 1:
                self.rule_from_last_log_entry(already_covered_rules)
                break
            if uncovered_all_logs_count <= 1:
                break
            start = datetime.utcnow()
            valid_keys = set(self.generation_param_info['event_normalizer_params']['valid_keys'])
            if self.key_filter:
                valid_keys.intersection_update(self.key_filter)
            data, single_value_columns = TableCreator.build_orange_table_from_es_logs(self.obs_query, valid_keys, True, self.all_logs_index, self.unique_logs_index)
            X, mapping = OneHot.encode(data, include_class=True)
            if X is None and single_value_columns:
                best_least_rule = self.rule_from_set(single_value_columns)
            else:
                names = {item: '{}={}'.format(var.name, val)
                         for item, var, val in OneHot.decode(mapping, data, mapping)}
                del data
                itemsets = dict(frequent_itemsets(X, itemset_frequency))
                if not itemsets:
                    self.low_frequency_remaining_rules(already_covered_rules)
                    break
                itemset_len_original = len(itemsets)
                if itemset_limit > 0 and len(itemsets) > itemset_limit:
                    itemsets = self.enforce_itemset_limit(itemset_limit, itemsets)
                time_elapsed = datetime.utcnow() - start

                print('Itemset mining time: %ds Itemsets to examine: %d Original Itemsets Count: %d' % (time_elapsed.total_seconds(), len(itemsets), itemset_len_original))
                start = datetime.utcnow()
                inputs = []
                chunk_size = math.ceil(len(itemsets) / (self.worker_threads*4))
                chunks = self.chunks(itemsets, chunk_size)
                logs_count_dict = {'original_all_logs_count': original_all_logs_count, 'original_unique_logs_count': original_unique_logs_count, 'uncovered_all_logs_count': uncovered_all_logs_count, 'uncovered_unique_logs_count': uncovered_unique_logs_count}
                coverate_rate_denominator = logs_count_dict[self.job['config']['abac_params']['metric']['coverage_rate_method']]
                for chunk in chunks:
                    inputs.append((chunk, mapping, names, coverate_rate_denominator, single_value_columns))
                results = pool.starmap(oa_calc.calc_overassignments, inputs)
                time_elapsed = datetime.utcnow() - start
                print('Itemset scoring time: %ds, itemsets examined: %d' % (time_elapsed.total_seconds(), len(itemsets)))
                best_least_rule = self.get_best_result(results)
                del itemsets
                del X

            already_covered_rules.append(best_least_rule)
            # self.merge_or_append_rule(best_least_rule, already_covered_rules)
            print('over_rate: %f, over_total: %d, under_total: %d, covR: %f, covC: %d, constraints: %d   %s' % (
                best_least_rule.overassignment_rate_all, best_least_rule.overassignment_total, best_least_rule.under_assignments, best_least_rule.coverage_rate, best_least_rule.all_log_entries, len(best_least_rule.constraints), ', '.join(i for i in sorted(best_least_rule.constraints))))
            print()
            self.remove_entires_covered_by_rule(best_least_rule)
        pool.close()
        es.indices.delete(index=self.unique_logs_index, ignore=[400, 404])
        es.indices.delete(index=self.all_logs_index, ignore=[400, 404])

        #print(json.dumps(already_covered_rules, cls=SetEncoder))
        mining_compelted_time = datetime.utcnow()
        main_time_elapsed = mining_compelted_time - main_start
        print('Time elapsed: %d' % main_time_elapsed.total_seconds())
        rules_summation_stats = {'sum_overassignment_total': 0,
                                 'sum_unique_log_entries': 0,
                                 'sum_all_log_entries': 0,
                                 'sum_allowed_events_count': 0}
        rules_as_dicts = []
        for rule in already_covered_rules:
            rules_summation_stats['sum_overassignment_total'] += rule.overassignment_total
            rules_summation_stats['sum_unique_log_entries'] += rule.unique_log_entries
            rules_summation_stats['sum_all_log_entries'] += rule.all_log_entries
            rules_summation_stats['sum_allowed_events_count'] += rule.allowed_events_count
            rule.constraints = list(rule.constraints)
            rule.q = json.dumps(rule.q, default=json_util.default)
            rules_as_dicts.append(rule.to_dict())
        # rules_as_dicts = sorted(rules_as_dicts, key=lambda x: x['overassignment_total'], reverse=True)
        result = {
            'rules_summation_stats': rules_summation_stats,
            'rules': rules_as_dicts,
            'total_possible_priv_states': self.total_possible_priv_states,
            'mining_time_elapsed_s': main_time_elapsed.total_seconds(),
            'mining_completed_at': mining_compelted_time,
            'generation_param_info_id': self.generation_param_info_id
        }
        rule_merger = RuleMerger()
        rule_merger.merge_policy(result['rules'])
        print(rules_summation_stats)
        return result

    def enforce_itemset_limit(self, itemset_limit, itemsets):
        counts = []
        for count in itemsets.values():
            counts.append(count)
        counts.sort(reverse=True)
        min_count = counts[itemset_limit]
        new_itemsets = {k: v for k, v in itemsets.items() if v > min_count}
        for k, v in itemsets.items():
            if v == min_count:
                new_itemsets[k] = v
            if len(new_itemsets) >= itemset_limit:
                break
        itemsets = new_itemsets
        return itemsets

    def low_frequency_remaining_rules(self, already_covered_rules):
        paginator = helpers.scan(es, query={"query": {"match_all": {}}}, index=self.unique_logs_index, doc_type='doc')
        for hit in paginator:
            rule_input = set()
            for k, v in hit['_source'].items():
                rule_input.add('%s=%s' % (k, v))
            rule = RuleEval()
            rule.set_constrants(rule_input)
            already_covered_rules.append(rule)
            self.remove_entires_covered_by_rule(rule)

    def rule_from_last_log_entry(self, already_covered_rules):
        rule_input = set()
        for k, v in self.es.search(index=self.unique_logs_index, doc_type='doc', body={"query": {"match_all": {}}})['hits']['hits'][0]['_source'].items():
            rule_input.add('%s=%s' % (k, v))
        rule = RuleEval()
        rule.set_constrants(rule_input)
        already_covered_rules.append(rule)

    def remove_entires_covered_by_rule(self, rule):
        es_constraints_to_delete_query = RuleUtils.create_query_convert_resource_to_type(rule.constraints)
        if 'size' in es_constraints_to_delete_query: del es_constraints_to_delete_query['size']
        self.es.delete_by_query(index=self.all_logs_index, doc_type='doc', body=es_constraints_to_delete_query, refresh=True, request_timeout=60)
        self.es.delete_by_query(index=self.unique_logs_index, doc_type='doc', body=es_constraints_to_delete_query, refresh=True, request_timeout=60)

if __name__ == "__main__":

    job = jobs_collection.find_one(filter={'_id': '2017-03-21_to_2017-04-20-0knNdL4qyu'})  #100000
    # job = jobs_collection.find_one(filter={'_id': '2017-03-21_to_2017-04-20-VAnDy0vRK3'})  # 1/100000
    job_utls.decode_query_dates(job)
    rule_miner = EnvFlatAbacRuleMiner(job)
    policy = rule_miner.mine_rules_for_window()
    policy['job'] = job_utls.encode_query_dates(job)
    policy['generation_param_info_id'] = rule_miner.generation_param_info_id
    policy_collection.replace_one({'_id': job['_id']}, policy, True)

