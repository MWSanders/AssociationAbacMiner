import base64
import copy
import datetime
import hashlib
import json

import pymongo
from bson import json_util

from src.config import config
from src.job.WindowGenerator import WindowGenerator

client = config.client #MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
db = config.db #client.calp
jobs_collection = config.jobs_collection
events_collection = config.events_collection
param_info_collection = config.param_universe_info_collection

class JobGenerator(object):
    def __init__(self):
        events_collection.create_index([('eventTime', pymongo.ASCENDING)])
                                        #('userIdentity.type', pymongo.ASCENDING)])
                                        # ("errorCode", pymongo.ASCENDING)])

    def generate_jobs(self, job_config):
        window_generator = WindowGenerator(job_config)
        windows = window_generator.generate_windows()
        source_query = job_config['source_query']
        perm_universe_query = copy.deepcopy(source_query)
        perm_universe_query['eventTime']['$gte'] = window_generator.calendar_start
        perm_universe_query['eventTime']['$lte'] = window_generator.calendar_end
        config_string = base64.b64encode(hashlib.sha1(json.dumps(job_config, sort_keys=True, default=json_util.default).encode('utf-8')).digest()).decode('utf-8')[:10]
        print('Generating jobs for config: %s' % config_string)
        for window in windows:
            obs_query = window.update_obs_query_with_window_bounds(source_query)
            opr_query = window.update_opr_query_with_window_bounds(source_query)
            obs_opr_query = window.update_obs_opr_query_with_window_bounds(source_query)

            id = '%s_to_%s-%s' % (window.obs_start.strftime('%Y-%m-%d'), window.obs_end_opr_start.strftime('%Y-%m-%d'), config_string)
            job = {'_id': id,
                   'job_type': 'mine_rules',
                   'calendar_start_end': '%s_%s' % (window_generator.calendar_start, window_generator.calendar_end),
                   'obs_query': json.dumps(obs_query, default=json_util.default),
                   'opr_query': json.dumps(opr_query, default=json_util.default),
                   'obs_opr_query': json.dumps(obs_opr_query, default=json_util.default),
                   'source_query': json.dumps(source_query, default=json_util.default),
                   'perm_universe_query': json.dumps(perm_universe_query, default=json_util.default),
                   'window': window.__dict__,
                   'state': 'NEW',
                   'created': datetime.datetime.now(),
                   'config': job_config,
                   'config_hash': config_string
                   }
            job['config']['source_query'] = json.dumps(source_query, default=json_util.default)
            try:
                jobs_collection.replace_one({'_id': job['_id']}, job, True)
            except:
                pass

if __name__ == "__main__":
    job_type = 'ABAC'
    job_gen = JobGenerator()
    if job_type == 'RBAC':
        possible_configs = {'use_resources': False,
                            'mine_method': 'RBAC',
                            'obs_days': [3, 7, 15, 30, 45, 60, 90, 120],
                            'opr_days': 1,
                            'calendar_start': '2017-03-21 00:00:00.000Z',
                            # 'calendar_end':   '2017-07-14 23:59:59.000Z',
                            'calendar_end': '2018-07-12 23:59:59.000Z',
                            'source_query': {"eventTime": {'$gte': '',
                                                           '$lte': ''},
                                             #"eventSource": "iam.amazonaws.com",
                                             #'userIdentity.type': 'IAMUser' #{'$in': ['IAMUser', 'AssumedRole']}#, #{'$ne': 'Root'},
                                             #'errorCode': {'$exists': False}
                                            },
                            'scoring_param_info_id': 'full_wresources_amftrue_pbftrue', #'short_40_nd_amftrue_pbftrue', #'common_scoring_nd_amftrue_pbftrue',  #'full_wresources_amftrue_pbftrue', #'full_common_scoring_nd_amftrue_pbftrue',
                                'abac_params': {
                                    'generation_param_info_id': 'full_wresources_amftrue_pbftrue', #'short_40_nd_amftrue_pbftrue', #'common_scoring_nd_amftrue_pbftrue', #'full_wresources_amftrue_pbftrue', #'full_rbac_nd_amftrue_pbftrue',
                                }
                            }
        for obs_days in possible_configs['obs_days']:
            job_config = copy.deepcopy(possible_configs)
            job_config['obs_days'] = obs_days
            job_gen.generate_jobs(job_config)
            print(job_config)
    if job_type == 'ABAC':
        generation_param_info_ids = [s for s in list(param_info_collection.find({}).distinct('_id')) if 'full' in s.lower() and 'rbac' not in s.lower()] #if 'pbftrue' in s]# []
        # for amf in [True]:
        #     for pbf in [False]:
        #         for id in ['corrfs_common_scoring', 'fs_b_userAgent', 'eventName']:
        #             id = id + '_amf%s_pbf%s' % (amf, pbf)
        #             id = id.lower()
        #             generation_param_info_ids.append(id)
        #         for i in range(1, 14):
        #             id = 'vf_score_fields_minus_lowest%d_amf%s_pbf%s' % (i, amf, pbf)
        #             id = id.lower()
        #             generation_param_info_ids.append(id)
        #         for i in range(1, 14):
        #             id = 'vf_score_fields_minus_highest%d_amf%s_pbf%s' % (i, amf, pbf)
        #             id = id.lower()
        #             generation_param_info_ids.append(id)
        possible_configs = {'use_resources': False,
                            # 'mine_method': 'RBAC',
                            'mine_method': 'ABAC',
                            'abac_params': {
                                # 'generation_param_info_id': ['common_scoring_nd_amftrue_pbftrue'], #['short_40_nd_amftrue_pbftrue'], #['short_wresources_abac_amftrue_pbftrue'],
                                'generation_param_info_id': ['full_wresources'], #['full_wresources_amftrue_pbftrue'],
                                'key_filter': [None],
                                              #  ['sourceIPAddress','sourceIPAddress_trunc','eventName','eventTime_weekday','eventTime_bin','eventSource','userIdentity_userName','userAgent'],
                                              #  ['eventName_bin','userIdentity_sessionContext_attributes_mfaAuthenticated','userAgent_bin','userIdentity_invokedBy','userAgent_general_bin','eventVersion','userIdentity_accessKeyId','requestParameters_path'],
                                              #  ['requestParameters_encryptionContext_PARAMETER_ARN','eventName_crud_bin','sourceIPAddress_bin','requestParameters_name','requestParameters_pipelineName','eventTime_weekend','apiVersion','requestParameters_maxResults']
                                              # ],
                                'itemset_freq':  [0.1], #[0.05, 0.1, 0.15, 0.2, 0.3], #[0.025, 0.05, 0.1, 0.2, 0.3], #[0.025, 0.05, 0.1, 0.2, 0.3],  # [0.0125, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3],
                                'itemset_limit': [600000], #75000],
                                'discard_common': [False],
                                'merge_method': None,
                                'metric': {
                                    'type': ['arithmetic_mean'],
                                    # 'type': ['Qrul_count', 'Qrul_freq'], #[, 'Qrul_freq'], #['arithmetic_mean'], #['arithmetic_mean_wsc'], # [ 'Qrul_count1_i', 'Qrul_freq_i', 'Qrul_freq1_i',  'Qrul_count1', 'Qrul_freq1'], #['l_dist'], #['harmonic_mean', 'arithmetic_mean'],
                                    'coverage_rate_method': ['uncovered_all_logs_count'], #'['uncovered_unique_logs_count'] , #], #'original_unique_logs_count'], # ['original_all_logs_count'],
                                    'beta': [1/4], #8, 4, 2, 1, 1/2, 1/4, 1/8, 1/16, 1/32, 1/64, 1/128, 1/256, 1/512, 1/1024, 1/2048, 1/4096, 1/8192, 1/16384, 1/32768, 1/65536, 1/131072, 1/262144, 1/524288],
                                    # 'beta':  [0.999975, 0.99993, 0.99992, 0.9998, 500000, 1000000, 5000000, 10000000, 50000000], #[1, 0.75, 0.5, 0.25, 0.01], #[1000, 100, 10, 1, 1/10, 1/100, 1/1000], #[256, 64, 16, 4, 1, 1/4, 1/16, 1/64, 1/256], #[1/2, 1/4, 1/8, 1/16, 1/32, 1/64, 1/128], #, 1/512, 1/1024], #[1/16],  #B<1 overR is more important, B>1 covR is more important
                                    #'gamma': [0, 1, 10, 1/10],
                                    'max_threshold': [50000000]
                                },
                            },

                            # 'scoring_param_info_id': 'common_scoring_nd_amftrue_pbftrue', #'common_scoring_nd_amftrue_pbftrue', #'short_40_nd_amftrue_pbftrue', #'short_wresources_abac_amftrue_pbftrue',
                            'scoring_param_info_id': 'full_wresources', #'full_wresources_amftrue_pbftrue','full_wresources_amftrue_pbftrue'
                            'bin_method': ['eqf-6'],  #config.hour_binning_methods,
                            'obs_days': [30],  #[7, 15, 30, 45, 60, 75, 90, 105, 120],
                            'opr_days': 1,
                            'calendar_start': '2016-01-01 00:43:06.000Z',
                            # 'calendar_end':   '2017-07-14 23:59:59.000Z',  #115 days,
                            'calendar_end': '2016-04-12 23:05:10.000Z',    #478 days
                            'source_query': {"eventTime": {'$gte': '',
                                                           '$lte': ''}
                                            }
                            }
        for bin_method in possible_configs['bin_method']:
            for itemset_freq in possible_configs['abac_params']['itemset_freq']:
                for itemset_limit in possible_configs['abac_params']['itemset_limit']:
                    for discard_common in possible_configs['abac_params']['discard_common']:
                        for coverage_rate_method in possible_configs['abac_params']['metric']['coverage_rate_method']:
                            for metric_type in possible_configs['abac_params']['metric']['type']:
                                for beta in possible_configs['abac_params']['metric']['beta']:
                                    for max_threshold in possible_configs['abac_params']['metric']['max_threshold']:
                                        for obs_days in possible_configs['obs_days']:
                                            for generation_param_info_id in possible_configs['abac_params']['generation_param_info_id']:
                                                for key_filter in possible_configs['abac_params']['key_filter']:
                                                    job_config = copy.deepcopy(possible_configs)
                                                    job_config['bin_method'] = bin_method
                                                    job_config['abac_params']['itemset_freq'] = itemset_freq
                                                    job_config['abac_params']['itemset_limit'] = itemset_limit
                                                    job_config['abac_params']['discard_common'] = discard_common
                                                    job_config['abac_params']['metric']['coverage_rate_method'] = coverage_rate_method
                                                    job_config['abac_params']['metric']['type'] = metric_type
                                                    job_config['abac_params']['metric']['beta'] = beta
                                                    job_config['abac_params']['metric']['max_threshold'] = max_threshold
                                                    job_config['obs_days'] = obs_days
                                                    job_config['abac_params']['generation_param_info_id'] = generation_param_info_id
                                                    job_config['abac_params']['key_filter'] = key_filter
                                                    job_config['abac_params']['add_missing_fileds'] = True if 'amftrue' in generation_param_info_id else False
                                                    job_config['abac_params']['pop_binned_fields'] = True if 'pbftrue' in generation_param_info_id else False
                                                    job_gen.generate_jobs(job_config)
                                                    print(job_config)