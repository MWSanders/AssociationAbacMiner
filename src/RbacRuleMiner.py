from src.config import config
from src.model.LogUniverseGenerator import *
from src.model.RuleEval import RuleEval
from src.model.RuleEval import SetEncoder

client = config.client
events_collection = config.events_collection
jobs_collection = config.jobs_collection

class RbacRuleMiner(object):
    def __init__(self, job, log_universe_builder=None):
        self.job = job
        self.use_resources = self.job['config']['use_resources']
        self.user_possible_privs = 1
        self.op_possible_privs = 1
        self.resource_possible_privs = 1
        self.obs_query = job['obs_query']
        self.event_normalizer = ConfigurableEventNormalizerNg(use_resources=self.use_resources, bin_method='simple-6', fields_to_bin=set(), valid_keys=['eventName', 'userIdentity_userName'], add_missing_fields=False, pop_binned_fields=False)
        self.total_possible_priv_states = None
        self.param_dependency_mmap = None
        self.type_resource_map = None
        self.worker_threads = 6
        # self.generation_param_info_id = job['config']['abac_params']['generation_param_info_id']
        # self.generation_param_info = param_universe_dao.load_universe_info(self.generation_param_info_id)
        # self.generation_event_normalizer = ConfigurableEventNormalizerNg.from_param_info(self.generation_param_info)


    def mine_rules_for_window(self):
        rules_as_dicts = []
        user_ops = {}
        main_start = datetime.now()
        event_count = 0
        for event in events_collection.find(self.obs_query, {'userIdentity.userName':1, 'eventName':1, 'eventSource':1, 'userAgent':1}):
            event_count += 1
            normd_event = self.event_normalizer.normalized_user_op_resource_from_event(event)
            RuleUtils.addMulti(user_ops, normd_event['userIdentity_userName'], normd_event['eventName'])

        mining_compelted_time = datetime.now()
        main_time_elapsed = mining_compelted_time - main_start
        for user, ops in user_ops.items():
            constraints = set()
            rule_eval = RuleEval()
            constraints.add('userIdentity_userName=%s' % user)
            for op in ops:
                constraints.add('eventName=%s' % op)
            rule_eval.set_constrants(constraints)
            rules_as_dicts.append(rule_eval.to_dict())

        result = {
            'rules_summation_stats': None, #rules_summation_stats,
            'rules': rules_as_dicts,
            # 'total_possible_priv_states': self.total_possible_priv_states,
            'mining_time_elapsed_s': main_time_elapsed.total_seconds(),
            'mining_completed_at': mining_compelted_time
        }
        # print(rules_summation_stats)
        return result

if __name__ == "__main__":
    job = jobs_collection.find_one(filter={'_id': '2017-03-21_to_2017-03-24-8z3ybsYLnN'})
    job_utls.decode_query_dates(job)
    rule_miner = RbacRuleMiner(job)
    result = rule_miner.mine_rules_for_window()
    print(result)
    result.pop('mining_completed_at')
    print(json.dumps(result, cls=SetEncoder))

