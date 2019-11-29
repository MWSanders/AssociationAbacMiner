import pymongo

from src.config import config
from src.job.WindowGenerator import WindowGenerator

client = config.client
jobs_collection = config.jobs_collection
summary_collection = config.summary_collection

class ROCPrinter(object):
    def __init__(self, config_hash):
        self.config_hash = config_hash
        window_generator = WindowGenerator()
        self.calendar_start = window_generator.calendar_start
        self.calendar_end = window_generator.calendar_end

    def print_roc_values(self):
        pass


if __name__ == "__main__":
    job_type = 'RBAC'
    if job_type == 'RBAC':
        print('config_hash, scoring_id, obs_days, score_windows, FPR, TNR, uTPR, cTPR, start, end')
        for agg_score in summary_collection.find({'config.mine_method': 'RBAC'}).sort([('config.obs_days', pymongo.ASCENDING)]):
            print('%s, %s, %d, %d, %f, %f, %f, %f, %s, %s' % (
                agg_score['config_hash'],
                agg_score['config']['scoring_param_info_id'],
                agg_score['config']['obs_days'],
                agg_score['days'],
                agg_score['uFPR'],
                1 - agg_score['uFPR'],
                agg_score['cRecall'],
                agg_score['uRecall'],
                agg_score['config']['calendar_start'].split(' ')[0],
                agg_score['config']['calendar_end'].split(' ')[0]))
    if job_type == 'ABAC':
        print('config_hash, obs_days, score_windows, FPR, TNR, uTPR, cTPR, uAMean, cAMean, itemset_freq, itemset_limit, metric, beta, max_threshold, bin_method, coverage_rate_method, scoring_param_info_id, generation_param_info_id, wsc, amf, pbf, score_time, mine_time, obs_days, score_windows, obs_days, score_windows')
        for agg_score in summary_collection.find({'config.mine_method': 'ABAC'}): #.sort([('config.obs_days', pymongo.ASCENDING)]):
            amf = True if 'amftrue' in agg_score['config']['abac_params']['generation_param_info_id'] else False
            pbf = True if 'pbftrue' in agg_score['config']['abac_params']['generation_param_info_id'] else False
            if 'abac_params' in agg_score['config']:
                covR_method = agg_score['config']['abac_params']['metric']['coverage_rate_method'] if 'coverage_rate_method' in agg_score['config']['abac_params']['metric'] else 'uncovered_all_logs_count'

                beta = agg_score['config']['abac_params']['metric']['beta']
                fpr = agg_score['uFPR']
                utpr = agg_score['uRecall']
                ctpr = agg_score['cRecall']
                tnr = 1 - agg_score['uFPR']
                umean = (utpr + tnr)/2
                cmean = (ctpr + tnr) / 2
                # w_mean = (tnr + (beta * tpr))/2
                # harmonic_mean = (2 * ((tpr * tnr) / (tpr + tnr)))
                # w_harmonic_mean = (1 + (beta * beta)) * ((tpr * tnr) / (((beta * beta) * tpr) + tnr))

                print('%s, %d, %d, %f, %f, %f, %f, %f, %f, %f, %d, %s, %f, %d, %s, %s, %s, %s, %f, %s, %s, %f, %f, %s, %s' % (
                    agg_score['config_hash'],
                    agg_score['config']['obs_days'],
                    agg_score['days'],
                    agg_score['uFPR'],
                    tnr,
                    agg_score['uRecall'],
                    agg_score['cRecall'],
                    umean,
                    cmean,
                    agg_score['config']['abac_params']['itemset_freq'],
                    agg_score['config']['abac_params']['itemset_limit'],
                    agg_score['config']['abac_params']['metric']['type'],
                    agg_score['config']['abac_params']['metric']['beta'],
                    agg_score['config']['abac_params']['metric']['max_threshold'],
                    agg_score['config']['bin_method'],
                    covR_method,
                    agg_score['config']['scoring_param_info_id'],
                    agg_score['config']['abac_params']['generation_param_info_id'],
                    agg_score['wsc'],
                    amf,
                    pbf,
                    agg_score['score_time'],
                    agg_score['mine_time'],
                    agg_score['config']['calendar_start'].split(' ')[0],
                    agg_score['config']['calendar_end'].split(' ')[0])
                )
