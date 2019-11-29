from src.config import config
import hashlib

client = config.client
scores_collection = config.scores_collection

score = scores_collection.find_one({'_id': '2017-10-14_to_2017-11-13-q9LCh7fDCr'})

overassignment_total = 0
for rule in score['policy']['rules']:
    overassignment_total += rule['overassignment_total']
    if rule['allowed_users'] == 0 or rule['allowed_ops'] == 0:
        print(rule['constraints'])

print('overassignment_total: %d' % overassignment_total)