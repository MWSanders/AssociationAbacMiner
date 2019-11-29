from src.config import config

client = config.client
job_collection = config.jobs_collection
scores_collection = config.scores_collection
#{'$ne':'COMPLETE'}
# for job in job_collection.find({'state':'MINING'},{'_id':1, 'state':1}):
#     score = scores_collection.find_one({'_id': job['_id']})
#     if score:
#         print('%s already scored, marking as COMPLETE' % job['_id'])
#         job_collection.find_one_and_update(filter={'_id': job['_id']}, update={'$set': {'state': 'COMPLETE'}})

not_scored = 0
for job in job_collection.find({'state':'NEW'},{'_id':1, 'state':1}):
    score = scores_collection.find_one({'_id': job['_id']})
    if score:
        print('%s has been scored, marking as COMPLETE' % job['_id'])
        job_collection.find_one_and_update(filter={'_id': job['_id']}, update={'$set': {'state': 'COMPLETE'}})
    if not score:
        not_scored +=1
        print('%s has NOT been scored: %d' % (job['_id'], not_scored))

print('Total not scored: %d' % not_scored)