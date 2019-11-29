from src.config import config

# log_level = os.getenv('LOG_LEVEL', 'INFO')
# logging.basicConfig(format='%(levelname)s:%(message)s', level=log_level)
# tracer = logging.getLogger('elasticsearch')
# tracer.setLevel(logging.CRITICAL) # or desired level

client = config.client
job_collection = config.jobs_collection
policy_collection = config.policy_colleciton
scores_collection = config.scores_collection

bad_policy_ids = []
for bad_policy in policy_collection.find({'rules': {'$exists':True, '$size':0}}):
    id = bad_policy['_id']
    bad_policy_ids.append(id)
    print(id)

for bad_id in bad_policy_ids:
    scores_collection.remove({'_id':bad_id})
    policy_collection.remove({'_id':bad_id})
    job_collection.find_one_and_update(filter={'_id': bad_id}, update={'$set': {'state': 'NEW'}})
