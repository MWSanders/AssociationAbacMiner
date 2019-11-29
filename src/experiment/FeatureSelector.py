from pymongo import MongoClient
from src.config import config
from src.model import RuleUtils
from skfeature.utility import construct_W
import json
from skfeature.utility import unsupervised_evaluation
from src.LogUniverseBuilder import LogUniverseBuilder
import numpy as np
from Orange.data import *
from orangecontrib.associate.fpgrowth import *
from sklearn.feature_selection import *
import scipy.io
from skfeature.function.sparse_learning_based import UDFS, MCFS, NDFS
from skfeature.utility import unsupervised_evaluation
from skfeature.utility.sparse_learning import feature_ranking
from skfeature.function.similarity_based import lap_score, SPEC
from skfeature.function.statistical_based import low_variance
# from src import EsUniverseWriter
from collections import *
from sklearn.feature_extraction import DictVectorizer
from src import TableCreator
from src.job import job_utls
from sortedcontainers import SortedList, SortedSet
import datetime
from src.experiment.PFA import PFA, PCAFeatures
import pandas as pd
import math
import six
from src.model import event_flattner

client = MongoClient(config.mongodb_conn, connectTimeoutMS=config.mongodb_timeout)
db = client.calp
events_collection = db.events
jobs_collection = db.abac_job_queue


class FeatureSelector(object):
    def __init__(self, job):
        self.job = job
        self.obs_query = job['perm_universe_query']
        self.log_universe_builder = LogUniverseBuilder(self.obs_query, job['config']['use_resources'], None, job['config']['bin_method'])
        self.event_normalizer = self.log_universe_builder.event_normalizer
        # self.index()

    def load_and_vectorize_raw(self, instance_limit):
        v = DictVectorizer(sparse=False)
        normed_events = []
        for event in events_collection.find(self.job['perm_universe_query']):
            keys_to_ignore = {'_id', 'eventID', 'requestID'}  #, 'requestParameters'])
            zero_variance_keys = {'recipientAccountId', 'awsRegion', 'environment', 'responseElements'}
            event['eventTime'] = self.event_normalizer.bin_hour(event['eventTime'].hour)
            # event['userAgent'] = self.event_normalizer.bin_userAgent(event['userAgent'])
            flat_event = event_flattner.flatten(event, "_", keys_to_ignore.union(zero_variance_keys))
            for k in ['userIdentity_type', 'userIdentity_accountId', 'userIdentity_sessionContext_attributes_creationDate']:
                flat_event.pop(k, None)
            normed_events.append(flat_event)
            if instance_limit and len(normed_events) >= instance_limit:
                break
        Xt = v.fit_transform(normed_events)
        print('Finished loading and raw transform of %d records' % len(normed_events))
        return Xt, v

    def index(self):
        # Index all and unique log events that occurred during the OBS period
        print('Indexing obs period logs')
        EsUniverseWriter.index_log_set(self.obs_query, self.log_universe_builder)

        # load service action types
        op_type_collection = db.ServiceOpResourceTypes
        self.service_action_types = {}
        for service_map in op_type_collection.find():
            id = service_map['_id']
            self.service_action_types[id] = {}
            self.service_action_types[id].update(service_map)

    def load_data(self):
        data = TableCreator.build_orange_table_from_es_logs(self.obs_query, self.job['config']['use_resources'], self.job['config']['abac_params']['discard_common'])
        X, mapping = OneHot.encode(data, include_class=True)
        names = {item: '{}={}'.format(var.name, val)
                 for item, var, val in OneHot.decode(mapping, data, mapping)}
        return data, mapping, names

    def load_and_vectorize(self, instance_limit=1000):
        v = DictVectorizer(sparse=False)
        normed_events = []
        for event in events_collection.find(self.job['obs_opr_query']):
            normd_event = self.log_universe_builder.event_normalizer.normalized_user_op_resource_from_event(event)
            normd_event.pop('resources')
            normed_events.append(normd_event)
            if len(normed_events) >= instance_limit:
                break
        # X = v.fit(normed_events)
        Xt = v.fit_transform(normed_events)
        return Xt, v

    def variance_threshold(self, X, vectorizor, threshold=0.05):
        # columns = X.columns
        feature_names = vectorizor.get_feature_names()
        selector = VarianceThreshold(threshold)
        start = datetime.datetime.now()
        result = selector.fit_transform(X)
        variances = selector.variances_
        variances_idx = np.argsort(variances)
        time_elapsed = datetime.datetime.now() - start
        mean_attribute_variance = {}
        total_attribute_variance = {}
        total_attribute_keys = Counter()
        for i in variances_idx:
            attribute = feature_names[i].split('=')[0]
            total_attribute_keys.update([attribute])
            if attribute not in total_attribute_variance:
                total_attribute_variance[attribute] = variances[i]
            else:
                total_attribute_variance[attribute] = total_attribute_variance[attribute] + variances[i]
            print('%s: %f' % (feature_names[i], variances[i]))
        for attribute in total_attribute_variance.keys():
            mean_attribute_variance[attribute] = total_attribute_variance[attribute] / total_attribute_keys[attribute]
        print('========== Attribute totals =================')
        for attribute_total in sorted(total_attribute_variance, key=total_attribute_variance.get, reverse=True):
            print('%s: %f' % (attribute_total, total_attribute_variance[attribute_total]))
        print('low_variance time: %ds, feature_keys: %d, OHE_feature_encodings: %d, instances: %d' % (time_elapsed.total_seconds(), len(total_attribute_keys), len(feature_names), len(X)))
        # labels = [columns[x] for x in selector.get_support(indices=True) if x]
        labels = [feature_names[x].split('=')[0] for x in selector.get_support(indices=True) if x]
        labels = SortedSet(labels)
        print(labels)


    def low_variance(self, X, vectorizor, threshold=0.05):
        feature_names = vectorizor.get_feature_names()
        start = datetime.datetime.now()
        selected_features = low_variance.low_variance_feature_selection(X, threshold * (1 - threshold))

        time_elapsed = datetime.datetime.now() - start
        print('low_variance time: %ds, features: %d, instances: %d' % (time_elapsed.total_seconds(), len(feature_names), len(X)))
        print(vectorizor.inverse_transform(selected_features))
        # sort the feature scores in an ascending order according to the feature scores
        idx = feature_ranking(selected_features)
        self.print_sorted_feature_names(vectorizor, idx)

    def spec_score(self, X, vectorizor):
        # specify the second ranking function which uses all except the 1st eigenvalue
        kwargs = {'style': 0}
        start = datetime.datetime.now()
        # obtain the scores of features
        feature_names = vectorizor.get_feature_names()
        score = SPEC.spec(X, **kwargs)
        time_elapsed = datetime.datetime.now() - start
        print('SPEC_score time: %ds, features: %d, instances: %d' % (time_elapsed.total_seconds(), len(feature_names), len(X)))
        # sort the feature scores in an ascending order according to the feature scores
        idx = SPEC.feature_ranking(score)
        self.print_sorted_feature_names(vectorizor, idx)

    def lap_score(self, X, vectorizor):
        # construct affinity matrix
        start = datetime.datetime.now()
        kwargs_W = {"metric": "euclidean", "neighbor_mode": "knn", "weight_mode": "heat_kernel", "k": 5, 't': 1}
        W = construct_W.construct_W(X, **kwargs_W)
        # obtain the scores of features
        feature_names = vectorizor.get_feature_names()
        score = lap_score.lap_score(X, W=W)
        time_elapsed = datetime.datetime.now() - start
        print('lap_score time: %ds, features: %d, instances: %d' % (time_elapsed.total_seconds(), len(feature_names), len(X)))
        # sort the feature scores in an ascending order according to the feature scores
        idx = lap_score.feature_ranking(score)
        self.print_sorted_feature_names(vectorizor, idx)

    def print_sorted_feature_names(self, vectorizor, idx):
        feature_names = vectorizor.get_feature_names()
        ranked_feature_set = SortedSet()
        for i in idx:
            # print(feature_names[i])
            ranked_feature_set.add(feature_names[i].split('=')[0])
        print(ranked_feature_set)

    def print_sorted_feature_values(self, vectorizor, idx):
        feature_names = vectorizor.get_feature_names()
        ranked_feature_set = SortedSet()
        for i in idx:
            # print(feature_names[i])
            ranked_feature_set.add(feature_names[i])
        print(ranked_feature_set)

    def udfs_score(self, X, vectorizor, gamma=0.1, num_cluster=20):
        # perform evaluation on clustering task
        num_fea = 100  # number of selected features
        # number of clusters, it is usually set as the number of classes in the ground truth

        # obtain the feature weight matrix
        print('Starting UDFS...')
        start = datetime.datetime.now()
        feature_names = vectorizor.get_feature_names()
        Weight = UDFS.udfs(X, gamma=gamma, n_clusters=num_cluster)
        print('UDFS finished...')
        time_elapsed = datetime.datetime.now() - start
        print('UDFS time: %ds, features: %d, instances: %d' % (time_elapsed.total_seconds(), len(feature_names), len(X)))

        # sort the feature scores in an ascending order according to the feature scores
        idx = feature_ranking(Weight)
        self.print_sorted_feature_names(vectorizor, idx)
        # print(idx)
        # obtain the dataset on the selected features
        # selected_features = X[:, idx[0:num_fea]]
        # print('Features: %d' % len(idx) )
        # print(idx)
        # return selected_features

    def mcfs_score(self, X, vectorizor, num_features=100, num_cluster=20):
        start = datetime.datetime.now()
        kwargs = {"metric": "euclidean", "neighborMode": "knn", "weightMode": "heatKernel", "k": 5, 't': 1}
        W = construct_W.construct_W(X, **kwargs)

        # obtain the feature weight matrix
        feature_names = vectorizor.get_feature_names()
        Weight = MCFS.mcfs(X, n_selected_features=num_features, W=W, n_clusters=num_cluster)
        time_elapsed = datetime.datetime.now() - start
        print('MCFS time: %ds, features: %d, instances: %d' % (time_elapsed.total_seconds(), len(feature_names), len(X)))

        # sort the feature scores in an ascending order according to the feature scores
        idx = MCFS.feature_ranking(Weight)
        self.print_sorted_feature_names(vectorizor, idx)

    def ndfs_score(self, X, vectorizor, num_cluster=20):
        kwargs = {"metric": "euclidean", "neighborMode": "knn", "weightMode": "heatKernel", "k": 5, 't': 1}
        W = construct_W.construct_W(X, **kwargs)

        # obtain the feature weight matrix
        print('Starting NDFS...')
        start = datetime.datetime.now()
        feature_names = vectorizor.get_feature_names()
        Weight = NDFS.ndfs(X, W=W, n_clusters=num_cluster)
        print('NDFS finished...')
        time_elapsed = datetime.datetime.now() - start
        print('NDFS time: %ds, features: %d, instances: %d' % (time_elapsed.total_seconds(), len(feature_names), len(X)))

        # sort the feature scores in an ascending order according to the feature scores
        idx = feature_ranking(Weight)
        self.print_sorted_feature_names(vectorizor, idx)

    def pfa(self, X, vectorizor, n_features=10):
        start = datetime.datetime.now()
        feature_names = vectorizor.get_feature_names()
        pfa = PFA(n_features)
        pfa.fit(X)
        time_elapsed = datetime.datetime.now() - start
        print('PFA time: %ds, clusters: %d, original features: %d, instances: %d' % (time_elapsed.total_seconds(), n_features, len(feature_names), len(X)))
        Xt = pfa.features_
        column_indicies = pfa.indices_
        self.print_sorted_feature_names(vectorizor, column_indicies)

    def pca(self, X, vectorizor, n_features=10):
        start = datetime.datetime.now()
        feature_names = vectorizor.get_feature_names()
        pfa = PCAFeatures(n_features)
        pfa.fit(X)
        time_elapsed = datetime.datetime.now() - start
        print('PCA time: %ds, clusters: %d, original features: %d, instances: %d' % (time_elapsed.total_seconds(), n_features, len(feature_names), len(X)))
        feature_scores = pfa.feature_scores
        idx = np.argsort(feature_scores, 0)
        self.print_sorted_feature_names(vectorizor, idx)

    def find_correlation2(self, X, vectorizor, thresh=0.95):
        # https://chrisalbon.com/machine_learning/feature_selection/drop_highly_correlated_features/
        start = datetime.datetime.now()
        df = pd.DataFrame(X)
        feature_names = vectorizor.get_feature_names()
        # Create correlation matrix
        corr_matrix = df.corr()
        # Select upper triangle of correlation matrix
        upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(np.bool))
        time_elapsed = datetime.datetime.now() - start
        print('Coorelation matrix creation time: %ds' % (time_elapsed.total_seconds()))
        correlation_counter = Counter()
        passing_scores = 0
        for i in range(0, len(upper)):
            row = upper.values[i]
            max_index = 0
            idx = np.argsort(row, 0)
            pass_threshold_correlations = []
            for j in range(0, len(row)):
                column = row[j]
                if feature_names[i].split('=')[0] != feature_names[j].split('=')[0]:
                    if math.fabs(column) >= thresh:
                        passing_scores += 1
                        print('%s -> %s: %f' % (feature_names[i], feature_names[j], column))
                        correlation_counter.update(['%s -> %s' % (feature_names[i].split('=')[0], feature_names[j].split('=')[0])])
        print('====Passing correlations: %d' % passing_scores)
        print(correlation_counter)

        # Find index of feature columns with correlation greater than 0.95
        # to_drop = [column for column in upper.columns if any(upper[column] > thresh)]
        # print('Correlated features: %d, Thresh: %f' %(len(to_drop), thresh))
        # self.print_sorted_feature_values(vectorizor, to_drop)

    def find_correlation(self, X, thresh=0.9):
        """
        Given a numeric pd.DataFrame, this will find highly correlated features,
        and return a list of features to remove
        params:
        - df : pd.DataFrame
        - thresh : correlation threshold, will remove one of pairs of features with
                   a correlation greater than this value
        """
        df = pd.DataFrame(X)
        corrMatrix = df.corr()
        corrMatrix.loc[:, :] = np.tril(corrMatrix, k=-1)

        already_in = set()
        result = []

        for col in corrMatrix:
            perfect_corr = corrMatrix[col][corrMatrix[col] > thresh].index.tolist()
            if perfect_corr and col not in already_in:
                already_in.update(set(perfect_corr))
                perfect_corr.append(col)
                result.append(perfect_corr)

        select_nested = [f[1:] for f in result]
        select_flat = [i for j in select_nested for i in j]
        return select_flat


if __name__ == "__main__":
    job = jobs_collection.find_one(filter={'_id': '2017-09-30_to_2017-10-30-819nwQtosi'})
    job_utls.decode_query_dates(job)
    selector = FeatureSelector(job)

    X, vectorizor = selector.load_and_vectorize_raw(100000)
    selector.variance_threshold(X, vectorizor)

    # for records in range(10000, 130000, 10000):
    #     start = datetime.datetime.now()
    #     # X, vectorizor = selector.load_and_vectorize(records)
    #     # X, vectorizor = selector.load_and_vectorize_raw(records)
    #     time_elapsed = datetime.datetime.now() - start
    #     print('Loaded: %d log entries with %d features in %ds' % (len(X), len(vectorizor.get_feature_names()), time_elapsed.total_seconds()))
    #     # selector.pca(X, vectorizor, 100)
    #     # selector.pfa(X, vectorizor, 100)
    #     # selector.variance_threshold(X, vectorizor)
    #     # selector.lap_score(X, vectorizor)
    #     # selector.spec_score(X, vectorizor)
    #     # selector.find_correlation2(X, vectorizor, 0.98)
    #     # selector.mcfs_score(X, vectorizor, 100, 20)
    #
    #     # selector.ndfs_score(X, vectorizor, 20)
    #     # selector.udfs_score(X=X, vectorizor=vectorizor, gamma=0.1, num_cluster=2)
    #     print()


