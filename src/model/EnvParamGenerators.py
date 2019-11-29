from src.eval.RuleEvaluator import RuleEvaluator
from src.model.EventEnumerator import *
from src.model.LogUniverseGenerator import *

client = config.client #MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
events_collection = config.events_collection
op_type_collection = config.service_op_resource_collection

class EnvGenerator(object):
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

class EnvParallelHashEnumerator(object):
    def __init__(self, pool, possible_params, statement, log_universe_generator, producer_threads=8, use_resources=True, param_info=None, event_normalizer=None):
        self.param_info = param_info
        self.use_resources = use_resources
        self.producer_threads = producer_threads
        self.statement = statement
        self.service_ops_types = log_universe_generator.service_ops_types
        self.possible_params = self.generate_current_params(possible_params, statement['constraints_map'])
        # self.param_dependency_mmap = log_universe_builder.param_dependency_mmap
        # self.type_resource_map = log_universe_builder.type_resource_map
        self.event_normalizer = event_normalizer

        self.user_events = []
        user_param_info = self.param_info.copy()
        user_param_info['possible_params'] = self.possible_params
        user_param_info = param_universe_dao.prune_param_info(user_param_info, self.param_info['valid_keys_sets']['valid_keys_user'])
        user_enumerator = EventEnumerator(user_param_info)
        for user in user_enumerator.generate_events():
            self.user_events.append(user)
        self.user_count = len(self.user_events)
        # print('User events generated: %d' % self.user_count)

        self.op_count = 0
        if self.user_count > 0:
            self.op_events = []
            op_param_info = self.param_info.copy()
            op_param_info['possible_params'] = self.possible_params
            param_universe_dao.prune_param_info(op_param_info, self.param_info['valid_keys_sets']['valid_keys_op'])
            op_enumerator = EventEnumerator(op_param_info)
            for op in op_enumerator.generate_events():
                self.op_events.append(op)
            self.op_count = len(self.op_events)
            # print('Op events generated: %d' % self.op_count)

        self.env_count = 0
        if self.op_count > 0:
            self.env_events = []
            env_param_info = self.param_info.copy()
            env_param_info['possible_params'] = self.possible_params
            param_universe_dao.prune_param_info(env_param_info, self.param_info['valid_keys_sets']['valid_keys_env'])
            env_enumerator = EventEnumerator(env_param_info)
            for env in env_enumerator.generate_events():
                self.env_events.append(env)
            self.env_count = len(self.env_events)
            # print('Env events generated: %d' % self.env_count)

        self.total_possible_privs = self.user_count * self.op_count * self.env_count


    def generate_current_params(self, possible_params, constraints_map):
        current_params = copy.deepcopy(possible_params)
        for key, value in constraints_map.items():
            if isinstance(value, list):
                current_params[key].intersection_update(value)
            elif isinstance(value, set):
                current_params[key].intersection_update(value)
            else:
                tmp_set = set()
                tmp_set.add(value)
                current_params[key] = tmp_set
        return current_params

    def single_threaded_calc_hashest(self, user_events, op_events, env_events, service_ops_types):
        hashes_list = []
        dicts_list = []
        dicts_list.append(user_events)
        dicts_list.append(op_events)
        dicts_list.append(env_events)
        rule_evaluator = RuleEvaluator(service_ops_types)
        for uor_dicts in itertools.product(*dicts_list):
            event = {}
            for x in uor_dicts:
                event.update(x)
            if self.use_resources:
                event = self.event_normalizer.flatten_resources(event)
            event_str = json.dumps(event, sort_keys=True).encode('utf-8')
            # if not rule_evaluator.rule_allows_event(event, self.statement['constraints_map']):
            #     logging.warning('Generated event that does not pass rule used in constructor: %s' % event_str)
            # else:
                # event_hash = hashlib.sha1(event_str).hexdigest()
                # event_hash = base64.b64encode(hashlib.sha1(event_str).digest()).decode('utf-8')
                # print(event_str)
                # event_hash = hash(event_str)
            event_hash = int(hashlib.sha256(event_str).hexdigest(), 16)
            hashes_list.append(event_hash)
        return hashes_list

    def calculate_allowed_hashes(self, pool, callback):
        allowed_hashes = []
        if self.producer_threads == 1:
            return self.single_threaded_calc_hashest(self.user_events, self.op_events, self.env_events, self.service_ops_types)

        largest_type = 'users'
        largest_list = self.user_events
        if len(self.op_events) > len(self.user_events):
            largest_type = 'ops'
            largest_list = self.op_events
        if len(self.env_events) > len(self.op_events):
            largest_type = 'env'
            largest_list = self.env_events

        chunk_size = math.ceil(len(largest_list) / (8 * self.producer_threads))
        resource_iters = [largest_list[x:x + chunk_size] for x in range(0, len(largest_list), chunk_size)]
        input_data_lists = []
        for data_list in resource_iters:
            if largest_type == 'users':
                input_data_lists.append((data_list, self.op_events, self.env_events, self.service_ops_types))
            elif largest_type == 'ops':
                input_data_lists.append((self.user_events, data_list, self.env_events, self.service_ops_types))
            elif largest_type == 'env':
                input_data_lists.append((self.user_events, self.op_events, data_list, self.service_ops_types))

        # pool = Pool(self.producer_threads)
        pool.starmap_async(self.single_threaded_calc_hashest, input_data_lists, callback=callback).wait()
        # pool.close()
        # pool.join()
        # return results
        # return itertools.chain(allowed_hashes)


if __name__ == "__main__":
    query = config.perm_universe_query
    #
    # service_ops_types = LogUniverseBuilder.service_ops_types
    # possible_params = LogUniverseBuilder.possible_params
    # param_dependency_mmap = LogUniverseBuilder.param_dependency_mmap
    # type_resource_map = LogUniverseBuilder.type_resource_map
    #
    # users = ops = resources = 0
    # user_gen = FlatUserGenerator(possible_params, param_dependency_mmap)
    # for user in user_gen.next(): users += 1
    # print('Users: %d' % users)
    # op_gen = FlatOpGenerator(possible_params, param_dependency_mmap)
    # for op in op_gen.next(): ops += 1
    # print('Ops: %d' % ops)
    # resource_gen = FlatResourceGenerator(possible_params, param_dependency_mmap, type_resource_map)
    # for resource in resource_gen.next(): resources += 1
    # print('Resources: %d' % resources)

