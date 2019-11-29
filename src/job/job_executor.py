import argparse
import os
import socket
import sys
import time

import pymongo
import traceback
from pymongo import ReturnDocument

# from src.FlatAbacRuleMiner import FlatAbacRuleMiner
from src.EnvAbacRuleMiner import EnvFlatAbacRuleMiner
from src.RbacRuleMiner import RbacRuleMiner
from src.eval.EnvPolicyEvaluator import EnvPolicyEvaluator
from src.model.LogUniverseGenerator import *
from src.model.RuleEval import SetEncoder

client = config.client
jobs_collection = config.jobs_collection
policy_collection = config.policy_colleciton
scores_collection = config.scores_collection
events_collection = config.events_collection
attrib_universe_collction = config.param_universe_info_collection

class JobExecutor:
    def __init__(self):
        mode = os.getenv('MODE', None)
        if mode == 'MINE_ONLY':
            self.do_mining = True
            self.do_scoring = False
        elif mode == 'SCORE_ONLY':
            self.do_mining = False
            self.do_scoring = True
        else:
            self.do_mining = True
            self.do_scoring = True
        self.filter_append = os.getenv('FILTER', None)
        if self.filter_append:
            self.filter_append = json.loads(self.filter_append)
        # self.param_universe_indexed = param_universe_indexed

    def encode_policy(self, policy):
        policy['rules'] = json.dumps(policy['rules'], cls=SetEncoder)


    def decode_policy(self, policy):
        policy['rules'] = json.loads(policy['rules'])

    def get_ip_addr(self):
        addr = (([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")] or [[(s.connect(("8.8.8.8", 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) + ["no IP found"])[0]
        return addr

    def score_created(self, job_id):
        scored_job = scores_collection.find_one({'_id': job_id})
        if scored_job:
            print('Job %s already completed so marking COMPLETE and skipping...' % job_id)
            jobs_collection.find_one_and_update(filter={'_id': job_id}, update={'$set': {'state': 'COMPLETE', 'last_updated_at': datetime.now()}}, return_document=ReturnDocument.AFTER)
            return True
        return False


    def get_next_job(self, job_id=None, new_type='NEW', in_progress_type='MINING'):
        job_started_time = datetime.now()
        update = {'$set': {'state': in_progress_type, 'started_at': job_started_time, 'last_updated_at': job_started_time, 'started_addr': socket.gethostname()}}
        sort = [('last_updated_at', pymongo.ASCENDING), ('created', pymongo.DESCENDING)]
        if self.filter_append:
            filter = self.filter_append.copy()
        else:
            filter = {}
        if job_id:
            filter = {'_id': job_id}
            job = jobs_collection.find_one_and_update(filter=filter, update=update, return_document=ReturnDocument.AFTER)
        else:
            filter['state'] = new_type
            job = jobs_collection.find_one_and_update(filter=filter, update=update, return_document=ReturnDocument.AFTER)
            if job:
                print('Starting %s job: %s' % (new_type, job['_id']))
            else:
                filter['state'] = new_type
                job = jobs_collection.find_one_and_update(filter=filter, update=update, sort=sort, return_document=ReturnDocument.AFTER)
                if job:
                    print('Only %s jobs are left, picking up job id: %s' % (in_progress_type, job['_id']))
                else:
                    print('There are no %s or %s jobs left in the queue.' % (new_type, in_progress_type))
                    time.sleep(5)
                    return None
        if self.score_created(job['_id']):
            return None
        print('CONFIG: %s' % job['config'])
        job_utls.decode_query_dates(job)
        return job

    def run_one_job(self, job_id=None):
        job = None
        try:
            complete_log_universe_builder = None
            #if do_mining get NEW or IN_PROGRESS
            if self.do_mining:
                job = self.get_next_job(job_id, 'NEW', 'MINING')
                if job:
                    existing_policy = policy_collection.find_one({'_id': job_id})
                    if existing_policy:
                        print('Policy %s already mined so skipping mining and scoring existing policy...' % job_id)
                        policy = policy_collection.find_one_and_update(filter={'_id': job_id}, update={'$set': {'job.state': 'MINED', 'job.last_updated_at': datetime.now()}}, return_document=ReturnDocument.AFTER)
                        job = jobs_collection.find_one_and_update(filter={'_id': job_id}, update={'$set': {'state': 'MINED', 'last_updated_at': datetime.now()}}, return_document=ReturnDocument.AFTER)
                    else:
                        job, policy = self.mine_job(job, complete_log_universe_builder)

            # if do_scoring get MINED or IN_SCORING
            if self.do_scoring:
                if not job:
                    job = self.get_next_job(job_id, 'MINED', 'SCORING')
                    if job:
                        job_id = job['_id']
                        policy = policy_collection.find_one({'_id': job_id})
                    elif jobs_collection.find_one(filter={'state': 'NEW'}) is None:
                        print('No jobs remaining, exiting...')
                        sys.exit(17)
                    else:
                        return
                scored_job = scores_collection.find_one({'_id': job_id})
                if scored_job:
                    print('Job %s has already been scored so marking COMPLETE and skipping...' % job_id)
                    jobs_collection.find_one_and_update(filter={'_id': job_id}, update={'$set': {'state': 'COMPLETE', 'last_updated_at': datetime.now()}}, return_document=ReturnDocument.AFTER)
                    return
                self.score_policy(policy)
        except Exception:
            traceback.print_exc()
            jobs_collection.find_one_and_update(filter={'_id': job['_id']}, update={'$set': {'last_updated_at': datetime.now(), 'state': 'ERROR'}})
        sys.exit(0)


    def mine_job(self, job, complete_log_universe_builder):
        use_resources = job['config']['use_resources']
        existing_policy = policy_collection.find_one({'_id': job['_id']})
        if existing_policy:
            print('Policy %s already completed so skipping mining and scoring existing policy...' % job['_id'])
            policy = policy_collection.find_one_and_update(filter={'_id': job['_id']}, update={'$set': {'job.state': 'MINED', 'job.last_updated_at': datetime.now()}}, return_document=ReturnDocument.AFTER)
            job = jobs_collection.find_one_and_update(filter={'_id': job['_id']}, update={'$set': {'state': 'MINED', 'last_updated_at': datetime.now()}}, return_document=ReturnDocument.AFTER)
        else:
            print('Building obs period parameter universe')
            if job['config']['mine_method'] == 'ABAC':
                miner = EnvFlatAbacRuleMiner(job)
            elif job['config']['mine_method'] == 'RBAC':
                miner = RbacRuleMiner(job)
            policy = miner.mine_rules_for_window()
            del miner
            policy['_id'] = job['_id']
        policy['job'] = job
        policy['state'] = 'MINED'

        # store policy score in Mongo
        job_utls.encode_query_dates(job)
        print('===STORING POLICY: %s' % job['_id'])
        policy_collection.replace_one({'_id': policy['_id']}, policy, True)
        job = jobs_collection.find_one_and_update(filter={'_id': job['_id']}, update={'$set': {'last_updated_at': datetime.now(), 'state': 'MINED'}}, return_document=ReturnDocument.AFTER)
        return job, policy

    def score_policy(self, policy):
        job = jobs_collection.find_one_and_update(filter={'_id': policy['_id']}, update={'$set': {'state': 'SCORING', 'last_updated_at': datetime.now()}}, return_document=ReturnDocument.AFTER)
        job_utls.decode_query_dates(policy['job'])
        print('===Policy scoring starting...')
        # job = policy['job']
        count_unique_hashes = job['config']['mine_method'] != 'RBAC'
        policy_evaluator = EnvPolicyEvaluator(policy, None, count_unique_hashes)
        scores = policy_evaluator.score_policy()
        print('===Policy scoring complete')
        scores['_id'] = job['_id']
        scores['policy'] = policy
        scores['policy']['job'] = job
        scores['job'] = job
        job_utls.encode_query_dates(job)
        print('===STORING SCORE: %s' % scores['_id'])
        scores_collection.replace_one({'_id': scores['_id']}, scores, True)
        print('===MARKING JOB COMPLETE %s' % job['_id'])
        jobs_collection.find_one_and_update(filter={'_id': job['_id']}, update={'$set': {'last_updated_at': datetime.now(), 'state': 'COMPLETE'}})



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch one job")
    parser.add_argument("--job_id", required=False, help="JobId")
    args = parser.parse_args()
    cwd = os.getcwd()
    sys.path.append(cwd)
    sys.path.append('..')
    print('Starting one job...')
    job_executor = JobExecutor()
    if args.job_id:
        job_executor.run_one_job(job_id=args.job_id)
    else:
        job_executor.run_one_job()
    # job_executor.run_one_job('2017-10-12_to_2017-10-19-yjne7TNnDG')
