from src.model.ConfigurableEventNormalizer import *
from src.model.EventEnumerator import *
from src.model.FlatParamGenerators import *
from src.util.AtomicCounter import AtomicCounter
from src.eval.WscCalculator import WscCalculator
from collections import Counter
import hashlib

client = MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
db = client.calp
policy_colleciton = db.abac_policies
scores_collection = db.abac_scores

class ConfigPolicySummarizer(object):
    def __init__(self):
        pass

    def summarize_policies(self):
        configs = {}
        for policy in policy_colleciton.find():
            config_hash = policy['job']['config_hash']
            if config_hash not in configs:
                configs[config_hash] = {'rules':0, 'rule_size':0, 'policies':0, 'constraints_counter': Counter(), 'field_counter': Counter()}
            configs[config_hash]['policies'] += 1
            configs[config_hash]['rules'] += len(policy['rules'])
            for rule in policy['rules']:
                configs[config_hash]['rule_size'] += len(rule['constraints'])
                for constraint in rule['constraints']:
                    configs[config_hash]['constraints_counter'].update([constraint])
                    key = constraint.split('=')[0]
                    if constraint.split('=')[1] == 'NONE':
                        continue
                    configs[config_hash]['field_counter'].update([key])
        print('config, policies, rules, constraints, avg_rules, avg_rule_size')
        for key, value in configs.items():
            avg_rules = value['rules']/value['policies']
            avg_rule_size = value['rule_size']/value['policies']
            print('%s, %d, %d, %d, %f, %f' % (key, value['policies'], value['rules'], value['rule_size'], avg_rules, avg_rule_size))
            print(value['field_counter'].most_common()[:10])
            print()


if __name__ == "__main__":
    policy_summarizer = ConfigPolicySummarizer()
    scores = policy_summarizer.summarize_policies()