from src.model.FlatParamGenerators import *
from src.model import RuleUtils
from src.config import config

client = config.client

class RuleMerger(object):
    def __init__(self):
        pass

    def make_constrants_map(self, constraints):
        constraints_map = {}
        for constraint in constraints:
            key = constraint.split('=')[0]
            value = constraint.split('=')[1]
            RuleUtils.addMulti(constraints_map, key, value)
        return constraints_map

    def merge_rules(self, rule_a, rule_b):
        rule_c = {}
        rule_c['constraints'] = copy.deepcopy(rule_a['constraints'])
        rule_c['constraints'].extend(rule_b['constraints'])
        rule_c['constraints'] = list(set(rule_c['constraints']))
        rule_c['constraints_map'] = self.make_constrants_map(rule_c['constraints'])
        return rule_c

    def can_merge_rules(self, rule_a, rule_b):
        if rule_a['constraints_map'].keys() == rule_b['constraints_map'].keys():
            matched_values = 0
            for key in rule_a['constraints_map'].keys():
                rule_a_values = set(rule_a['constraints_map'][key])
                rule_b_values = set(rule_b['constraints_map'][key])
                if rule_a_values == rule_b_values:
                    matched_values += 1
            if matched_values >= len(rule_a['constraints_map']) - 1:
                return True
        return False

    def find_and_merge(self, rules):
        for i in range(0, len(rules)):
            outer_rule = rules[i]
            for j in range(i+1, len(rules)):
                inner_rule = rules[j]
                if self.can_merge_rules(outer_rule, inner_rule):
                    new_rule = self.merge_rules(outer_rule, inner_rule)
                    return (i, j, new_rule)
        return None

    def merge_policy(self, rules):
        merges = 0
        while True:
            rule_tuple = self.find_and_merge(rules)
            if rule_tuple is None:
                break
            i, j, new_rule = rule_tuple
            rules[i] = new_rule
            del rules[j]
            merges += 1
        print('Rule merging complete, merged %d rules' % merges)
        for rule in rules:
            rule['constraints'] = list(rule['constraints'])
            for k,v in rule['constraints_map'].items():
                rule['constraints_map'][k] = list(v)
        return rules

if __name__ == "__main__":
    policy_colleciton = config.policy_colleciton
    policy = policy_colleciton.find_one({'_id': '2017-11-03_to_2017-12-03-0OLMks18Qn'}) # ABAC
    # policy = policy_colleciton.find_one({'_id': '2017-10-05_to_2017-10-19-ci6MdHhEjJ'})  # RBAC
    rule_merger = RuleMerger()
    rule_merger.merge_policy(policy['rules'])
    print(json.dumps(policy['rules']))
    print(policy['rules'])




