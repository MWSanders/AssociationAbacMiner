import os

from src.eval.WscCalculator import WscCalculator
from src.model.EnvParamGenerators import *
from src.util.AtomicCounter import AtomicCounter
from multiprocessing.pool import Pool

client = config.client
scores_collection = config.scores_collection
dynamic_params_cache = config.dynamic_params_cache

class EnvPolicyEvaluator(object):
    def __init__(self, policy, event_normalizer=None, count_unique_hashes=False, param_info=None):
        self.count_unique_hashes = count_unique_hashes
        self.policy = policy
        self.job = policy['job']
        # self.logger = config.get_logger(self.job['_id'])
        self.events_collection = config.events_collection
        self.generation_param_info = param_universe_dao.load_universe_info(policy['job']['config']['abac_params']['generation_param_info_id'])
        self.generation_event_normalizer = ConfigurableEventNormalizerNg.from_param_info(self.generation_param_info)
        self.scoring_param_info = param_universe_dao.load_universe_info(policy['job']['config']['scoring_param_info_id']) if param_info is None else param_info
        self.scoring_event_normalizer = ConfigurableEventNormalizerNg.from_param_info(self.scoring_param_info) if event_normalizer is None else event_normalizer
        self.log_universe_generator = LogUniverseGenerator(self.scoring_event_normalizer, policy['job']['obs_opr_query'])
        self.total_possible_priv_states = 0 # self.scoring_param_info['total_possible_priv_states']
        self.inner_allowed_hashes = set()
        self.wsc_calculator = WscCalculator(self.scoring_param_info)
        self.calculate_wsc()
        # self.all_allowed_hashes = SortedList()
        # self.all_allowed_hashes._reset(2000)
        # self.all_allowed_hashes = SortedSet()

    def policy_allows_event(self, policy, event, rule_evaluator):
        for statement in policy:
            if rule_evaluator.rule_allows_event(event, statement['constraints_map']):
                return True
        return False

    def calculate_wsc(self):
        self.policy['wsc'] = self.wsc_calculator.compute_policy_wsc(self.policy)
        for rule in self.policy['rules']:
            rule['wsc'] = self.wsc_calculator.compute_rule_wsc(rule)

    def chunks(self, data, SIZE=10000):
        # it = iter(data)
        data.sort()
        for i in range(0, len(data), SIZE):
            yield {k for k in data[i:i+SIZE]}
            # yield {k for k in itertools.islice(it, SIZE)}

    def chunk_params(self, possiblie_params, partition_field, chunks=8):
        # chunks = chunks * 2
        chunk_size = math.floor(len(possiblie_params[partition_field]) / chunks)
        operations = list(possiblie_params[partition_field])
        for ops_chunk in self.chunks(operations, chunk_size):
            current_params_chunk = possiblie_params.copy()
            current_params_chunk[partition_field] = ops_chunk
            yield current_params_chunk

    def store_dynamic_param_info(self, id, obs_id, dynamic_keys_hash, scoring_param_info):
        tmp_param_info = copy.deepcopy(self.scoring_param_info)
        tmp_param_info['dyanmic_keys_hash'] = dynamic_keys_hash
        tmp_param_info['obs_opr_id'] = obs_id
        tmp_param_info['param_info_id'] = tmp_param_info['_id']
        param_universe_dao.encode_existing_param_info(tmp_param_info)
        tmp_param_info.pop('_id', None)
        print('Storing dynamic params cache %s' % id)
        dynamic_params_cache.replace_one({'_id': id}, tmp_param_info, True)

    def generate_cache_id(self, scoring_param_info):
        dynamic_key_list = sorted(self.scoring_param_info['valid_keys_sets']['dynamic_keys'])
        dynamic_keys_hash = hashlib.sha1(json.dumps(dynamic_key_list, sort_keys=True).encode('utf-8')).hexdigest()[:10]
        valid_key_list = sorted(self.scoring_param_info['valid_keys_sets']['valid_keys'])
        valid_keys_hash = hashlib.sha1(json.dumps(valid_key_list, sort_keys=True).encode('utf-8')).hexdigest()[:10]
        dynamic_keys_hash = '%s-%s' % (valid_keys_hash, dynamic_keys_hash)
        obs_id = '%s_to_%s' % (self.job['window']['obs_start'].strftime("%Y-%m-%d"), self.job['window']['opr_end'].strftime("%Y-%m-%d"))
        id = '%s|%d|%s' % (obs_id, len(self.scoring_param_info['valid_keys_sets']['dynamic_keys']), dynamic_keys_hash)
        return id, obs_id, dynamic_keys_hash

    def count_positives(self, perm_universe_query, rules):
        if self.scoring_param_info['valid_keys_sets']['dynamic_keys']:
            id, obs_id, dynamic_keys_hash = self.generate_cache_id(self.scoring_param_info)
            cached_params = dynamic_params_cache.find_one({'_id': id})
            if cached_params:
                self.scoring_param_info = param_universe_dao.decode_param_info(cached_params)
                print('Cached params loaded for %s' % id)
            else:
                print('Generating param universe based on dynamic keys for id: %s' % id)
                prune_params_to_current = param_universe_dao.universe_period_intersection(self.scoring_event_normalizer, self.scoring_param_info, self.scoring_param_info['valid_keys_sets']['dynamic_keys'])
                prune_params_to_current.__next__()
                for event in events_collection.find(self.policy['job']['obs_opr_query']):
                    prune_params_to_current.send(event)
                prune_params_to_current.close()
                self.store_dynamic_param_info(id, obs_id, dynamic_keys_hash, self.scoring_param_info)
        possible_params = copy.deepcopy(self.scoring_param_info['possible_params'])
        unique_hashes_all_chunks = 0
        possible_summed_permutations_to_check_all_chunks = 0
        start_all_chunks = datetime.now()
        total_events_generated = 0
        if os.cpu_count() == 20:
            workers = 7
        elif os.cpu_count() == 16:
            workers = 7
        else:
            workers = 7 #int(os.cpu_count())-1
        pool = Pool(workers)
        priv_size_calculator = EnvParallelHashEnumerator(pool, possible_params, {'constraints_map':{}}, self.log_universe_generator, workers, self.job['config']['use_resources'], self.scoring_param_info, self.scoring_event_normalizer)
        self.total_possible_priv_states = priv_size_calculator.total_possible_privs
        outer_chunk_num = 0
        if self.count_unique_hashes:
            outer_params_chunks = self.chunk_params(possible_params, 'eventName', 10)
            # outer_params_chunks = self.chunk_params(possible_params, 'eventSource', workers)
        else:
            outer_params_chunks = [possible_params]
        for outer_params_chunk in outer_params_chunks:
            inner_chunk_num = 0
            if self.count_unique_hashes:
                inner_chunks = self.chunk_params(outer_params_chunk, 'userIdentity_userName', 10)
                # inner_chunks = self.chunk_params(outer_params_chunk, 'userIdentity_userName', workers)
            else:
                inner_chunks = [outer_params_chunk]
            for inner_params_chunk in inner_chunks:
                self.inner_allowed_hashes = set()
                possible_summed_permutations_to_check = 0
                total_over_generated = 0
                policy_start = datetime.now()
                statment_num = 0
                for statement in rules:
                    print('Chunk: %d:%d, Statement: %d' % (outer_chunk_num, inner_chunk_num, statment_num))
                    start = datetime.now()
                    print(statement['constraints_map'])
                    statment_num += 1

                    hash_init_start = datetime.now()
                    uor_gen = EnvParallelHashEnumerator(pool, inner_params_chunk, statement, self.log_universe_generator, workers, self.job['config']['use_resources'], self.scoring_param_info, self.scoring_event_normalizer)
                    estimated_events = uor_gen.user_count * uor_gen.op_count * uor_gen.env_count
                    hash_init_elapsed = datetime.now() - hash_init_start
                    print('HashEnumerator Init Time %ds, Generator Counts, U: %d, O: %d, R: %d, Estimated: %d' % (hash_init_elapsed.total_seconds(), uor_gen.user_count, uor_gen.op_count, uor_gen.env_count, estimated_events))
                    possible_summed_permutations_to_check += estimated_events
                    self.current_events_generated = 0
                    if self.count_unique_hashes and estimated_events > 0:
                        self.current_events_generated = AtomicCounter()
                        uor_gen.calculate_allowed_hashes(pool, self.add_hash_values_to_set)
                        self.current_events_generated = self.current_events_generated.get_value()
                    else:
                        self.current_events_generated = uor_gen.user_count * uor_gen.op_count * uor_gen.env_count
                    total_events_generated += self.current_events_generated
                    # self.update_statement_metrics(statement, uor_gen)
                    time_elapsed = datetime.now() - start
                    poliocy_time_elapsed = datetime.now() - policy_start
                    print('Statement Time: %ds, Actual Events(current): %d, Actual Events(total): %d, Unique Events(total): %d, Over Generated(Total): %d' %
                                     (time_elapsed.total_seconds(), self.current_events_generated, total_events_generated, len(self.inner_allowed_hashes), total_over_generated))
                    print('Policy Time so far: %ds' % poliocy_time_elapsed.total_seconds())
                    print('UniqueAllowed: %d, PossiblePermsToCheck: %d' % (len(self.inner_allowed_hashes), possible_summed_permutations_to_check))
                    print('')
                unique_hashes_all_chunks += len(self.inner_allowed_hashes)
                print('Chunk: %d:%d, Allowed Unique Hashes: %d' % (outer_chunk_num, inner_chunk_num, len(self.inner_allowed_hashes)))
                print('')
                possible_summed_permutations_to_check_all_chunks += possible_summed_permutations_to_check
                inner_chunk_num += 1
                del self.inner_allowed_hashes
            outer_chunk_num += 1
        pool.close()
        pool.join()
        time_elapsed_all_chunks = datetime.now() - start_all_chunks
        print('Total Time: %ds, UniqueAllowed: %d, PossiblePermsToCheck: %d' % (time_elapsed_all_chunks.total_seconds(), unique_hashes_all_chunks, possible_summed_permutations_to_check_all_chunks))
        if self.count_unique_hashes:
            return unique_hashes_all_chunks
        else:
            return total_events_generated

    def update_statement_metrics(self, statement, uor_gen):
        if uor_gen.product_size > 0:
            if 'opr_eval' not in statement:
                statement['opr_eval'] = {}
            if 'allowed_ops' not in statement['opr_eval']:
                statement['opr_eval']['allowed_ops'] = uor_gen.op_count
            else:
                statement['opr_eval']['allowed_ops'] += uor_gen.op_count
            if 'allowed_users' not in statement['opr_eval']:
                statement['opr_eval']['allowed_users'] = uor_gen.user_count
            else:
                statement['opr_eval']['allowed_users'] += uor_gen.user_count
            if 'allowed_resources' not in statement['opr_eval']:
                statement['opr_eval']['allowed_resources'] = uor_gen.env_count
            else:
                statement['opr_eval']['allowed_resources'] += uor_gen.env_count
            if 'allowed_events_count' not in statement['opr_eval']:
                statement['opr_eval']['allowed_events_count'] = self.current_events_generated
            else:
                statement['opr_eval']['allowed_events_count'] += self.current_events_generated

    def add_hash_values_to_set(self, values):
        while len(values) > 0:
            hashes_list = values.pop()
            for value in hashes_list:
                if value not in self.inner_allowed_hashes:
                    self.inner_allowed_hashes.add(value)

            self.current_events_generated.increment(len(hashes_list))
            # self.all_allowed_hashes.update(hashes_list)

    def score_policy(self):
        cTP = cFN = 0
        unique_TP_hashes = set() #{}
        unique_FN_hashes = set() #{}
        rule_evaluator = RuleEvaluator(self.log_universe_generator.service_ops_types)
        for event in self.events_collection.find(self.policy['job']['opr_query']):
            norm_event = self.generation_event_normalizer.normalized_user_op_resource_from_event(event)
            event_hash = hashlib.sha1(json.dumps(norm_event, sort_keys=True).encode('utf-8')).hexdigest()
            if self.policy_allows_event(self.policy['rules'], norm_event, rule_evaluator):
                unique_TP_hashes.add(event_hash) #[event_hash] = event_json
                cTP += 1
            else:
                unique_FN_hashes.add(event_hash) #[event_hash] = event_json
                cFN += 1

        start = datetime.now()
        perm_universe_query = self.policy['job']['obs_opr_query']
        opr_source_query = self.policy['job']['opr_query']
        rules = self.policy['rules']
        positive_hashes = self.count_positives(perm_universe_query, rules)
        end = datetime.now()
        time_elapsed = end - start

        uPos = positive_hashes
        uTP = len(unique_TP_hashes)
        uFN = len(unique_FN_hashes)
        uFP = positive_hashes - len(unique_TP_hashes)
        uTN = self.total_possible_priv_states - (uTP + uFN + uFP)
        print("Policy Time: %ds, uPos: %d, uTP: %d, uFP: %d uFN: %d uTN: %d, cTP: %d cFN: %d" % (time_elapsed.total_seconds(), uPos, uTP, uFP, uFN, uTN, cTP, cFN))
        #  The case where all privileges are denied is redefined to be Precision = 1 because there is no possibility of over-privilege
        if (uTP + uFP) == 0:
            uPrecision = 1
        else:
            uPrecision = uTP / (uTP + uFP)
        # The case where all privileges are granted, or no privs were exercised is redefined to be Recall = 1 because there is no possibility of under-privilege.
        if (uTP + uFN) == 0:
            uRecall = 1
        else:
            uRecall = uTP / (uTP + uFN)
        if (cTP + cFN) == 0:
            cRecall = 1
        else:
            cRecall = cTP / (cTP + cFN)
        uFPR = uFP / (uFP + uTN)
        specificity = uTN / (uTN + uFP)
        accuracy = (uTP + uTN) / self.total_possible_priv_states
        print('Precision: %f Recall: %f Specificity: %f Accuracy: %f' % (uPrecision, uRecall, specificity, accuracy))
        result = {'uPos': uPos, 'uTP': uTP, 'uFN': uFN, 'uFP': uFP, 'uTN': uTN, 'uFPR': uFPR, 'uPrecision': uPrecision, 'uRecall': uRecall, 'uSpecificity': specificity, 'uAccuracy': accuracy,
                  'cTP': cTP, 'cFN': cFN, 'cRecall': cRecall,
                  'scoring_time': time_elapsed.total_seconds()}
        return result


if __name__ == "__main__":
    policy_colleciton = config.policy_colleciton
    policy = policy_colleciton.find_one({'_id': '2018-03-31_to_2018-04-30-b8WvT3Su+E'}) # ABAC
    # policy = policy_colleciton.find_one({'_id': '2017-10-05_to_2017-10-19-ci6MdHhEjJ'})  # RBAC
    policy['job'] = job_utls.decode_query_dates(policy['job'])
    count_unique_hashes = policy['job']['config']['mine_method'] != 'RBAC'
    count_unique_hashes = True
    policy_evaluator = EnvPolicyEvaluator(policy, None, count_unique_hashes, None)
    scores = policy_evaluator.score_policy()
    print(scores)

    scores_collection.replace_one({'_id': policy['_id']}, scores, True)

