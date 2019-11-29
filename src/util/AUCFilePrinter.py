import numpy as np
from sklearn import metrics
import pandas as pd
import os


from src.config import config

# client = config.client
# summary_collection = config.summary_collection

def deduplicate_summaries(summaries):
    result = []
    summaries.sort(key=lambda x: x['config']['abac_params']['itemset_limit'], reverse=False)
    summaries.sort(key=lambda x:x['config']['abac_params']['metric']['beta'])
    last_beta = None
    for summary in summaries:
        if summary['config']['abac_params']['metric']['beta'] == last_beta:
            continue
        last_beta = summary['config']['abac_params']['metric']['beta']
        result.append(summary)
    return result

if __name__ == "__main__":
    path = '../resources/csv/'
    files = os.listdir(path)
    for filename in files:
        if not filename.endswith('.csv'):
            continue
        filename = path + filename
        tprs = []
        tprs.append(1.0)
        fprs = []
        fprs.append(1.0)
        with open(filename) as f:
            for line in f:
                fpr = float(line.split()[0])
                tpr = float(line.split()[1])
                tprs.append(tpr)
                fprs.append(fpr)

        tprs.append(0.0)
        fprs.append(0.0)
        fpr = np.array(fprs)
        tpr = np.array(tprs)
        mean_auc = metrics.auc(fpr, tpr, reorder=True)
        print('%s: %f' % (filename, mean_auc))

    # open('../resources/uncoveredUnique.csv')
    # for gen_param_name in summary_collection.distinct('config.abac_params.generation_param_info_id'):
    #     summaries = list(summary_collection.find({'config.abac_params.generation_param_info_id':gen_param_name}))

    #     summaries = deduplicate_summaries(summaries)
    #     for summary in summaries:
    #         tprs.append(summary['cRecall'])
    #         fprs.append(summary['uFPR'])
    #     tprs.append(1.0)
    #     fprs.append(1.0)
    #     fpr = np.array(fprs)
    #     tpr = np.array(tprs)
    #     mean_auc = metrics.auc(fpr, tpr, reorder=True)
    #     print('%s: %f' % (gen_param_name, mean_auc))
