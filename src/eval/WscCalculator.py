from src.model.FlatParamGenerators import *
from src.config import config

client = config.client
scores_collection = config.scores_collection
policy_collection = config.policy_colleciton

class WscScoreUpdater(object):
    def __init__(self, param_info, w_u=1.0, w_o=1.0, w_r=1.0):
        self.w_u = w_u
        self.w_o = w_o
        self.w_r = w_r
        self.param_info = param_info

    def update_scores(self):
        wsc_calc = WscCalculator(self.param_info, self.w_u, self.w_o, self.w_r)
        wsc_id = 'wsc_%d%d%d' % (self.w_u, self.w_o, self.w_r)
        for abac_score in scores_collection.find():
            policy = abac_score['policy']
            wsc = wsc_calc.compute_policy_wsc(policy)
            scores_collection.find_one_and_update(filter={'_id': abac_score['_id']}, update={'$set': {wsc_id: wsc}})


class WscCalculator(object):
    def __init__(self, param_info, w_u=1.0, w_o=1.0, w_r=1.0):
        self.w_u = w_u
        self.w_o = w_o
        self.w_r = w_r
        self.param_info = param_info

    def compute_rule_wsc(self, rule):
        user_constraints = set()
        op_constraints = set()
        resource_constraints = set()
        for constraint in rule['constraints']:
            key = constraint.split('=')[0]
            if key.startswith('requestParameters_name'):
                resource_constraints.add(constraint)
            elif key in self.param_info['valid_keys_sets']['valid_keys_user']:
                user_constraints.add(constraint)
            elif key in self.param_info['valid_keys_sets']['valid_keys_op']:
                op_constraints.add(constraint)
        wsc_r = self.w_u * len(user_constraints) + self.w_o * len(op_constraints) + self.w_r * len(resource_constraints)
        return wsc_r

    def compute_policy_wsc(self, policy):
        wsc_p = 0
        for i in range(0, len(policy['rules'])):
            rule = policy['rules'][i]
            wsc_r = self.compute_rule_wsc(rule)
            # print('Rule: %d, WSC: %f' % (i, wsc_r))
            wsc_p += wsc_r
        return wsc_p

if __name__ == "__main__":
    param_info = param_universe_dao.load_universe_info('25fields_common_scoring')

    policy = policy_colleciton.find_one({'_id': '2017-09-30_to_2017-10-30-e7S9tiJWCs'})  # ABAC
    # wsc_calc = WscCalculator(param_info)
    # wsc = wsc_calc.compute_policy_wsc(policy)
    # print('Total WSC: %f' % wsc)

    score_updater = WscScoreUpdater(param_info)
    score_updater.update_scores()

    # policy['job'] = job_utls.decode_query_dates(policy['job'])
    # policy_evaluator = FlatPolicyEvaluator(policy, None, True, None)
    # scores = policy_evaluator.score_policy()
    # print(scores)
    #
    # scores_collection.replace_one({'_id': policy['_id']}, scores, True)

