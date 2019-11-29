import json

from bson import json_util

from src.config import config
from src.job.WindowGenerator import WindowGenerator

client = config.client
jobs_collection = config.jobs_collection
scores_collection = config.scores_collection
summary_collection = config.summary_collection

class ScoreAggregator(object):
    def __init__(self, config_hash, job_config):
        self.config_hash = config_hash
        self.job_config = job_config
        window_generator = WindowGenerator(self.job_config)
        self.calendar_start = window_generator.calendar_start
        self.calendar_end = window_generator.calendar_end

    def aggregate_scores(self):
        source_query = self.job_config['source_query']
        perm_universe_query = json.loads(source_query)
        perm_universe_query['eventTime']['$gte'] = self.calendar_start
        perm_universe_query['eventTime']['$lte'] = self.calendar_end
        scores = []
        summary_score = {'uPos': 0, 'uTP': 0, 'uFN': 0, 'uFP': 0, 'uTN': 0, 'cTP': 0, 'cFN': 0, 'cRecall': 0,
                'uFPR': 0, 'uPrecision': 0, 'uRecall': 0, 'uSpecificity': 0, 'uAccuracy': 0, 'wsc':0, 'mine_time': 0, 'score_time': 0}
        scores_count = 0
        job_config = None
        for score in scores_collection.find({'job.config_hash': self.config_hash}):
            job_config = score['job']['config']
            scores_count += 1
            scores.append(score)
            for key in summary_score.keys():
                if key == 'wsc':
                    summary_score[key] += score['policy'][key]
                elif key == 'mine_time':
                    summary_score[key] += score['policy']['mining_time_elapsed_s']
                elif key == 'score_time':
                    summary_score[key] += score['scoring_time']
                else:
                    summary_score[key] += score[key]

        for key in summary_score.keys():
            summary_score[key] = summary_score[key] / scores_count
        summary_score['days'] = scores_count

        print(summary_score)
        scores.sort(key=lambda x: x['_id'], reverse=False)
        # summary_score['scores'] = scores
        summary_score['_id'] = '%s_%s-%s' % (self.calendar_start.strftime('%Y-%m-%d'), self.calendar_end.strftime('%Y-%m-%d'), self.config_hash)
        summary_score['query'] = json.dumps(perm_universe_query, default=json_util.default)
        summary_score['config_hash'] = self.config_hash
        summary_score['config'] = job_config
        summary_collection.replace_one({'_id': summary_score['_id']}, summary_score, True)


if __name__ == "__main__":
    config_hashes = scores_collection.distinct('job.config_hash')
    for hash in config_hashes:
        if not hash:
            continue
        current_score = scores_collection.find_one({'job.config_hash': hash})
        score_aggregator = ScoreAggregator(hash, current_score['job']['config'])
        score_aggregator.aggregate_scores()