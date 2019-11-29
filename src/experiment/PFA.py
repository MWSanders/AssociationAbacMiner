from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from collections import defaultdict
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.preprocessing import StandardScaler
import math

class PFA(object):
    def __init__(self, n_features, q=None):
        self.q = q
        self.n_features = n_features

    def fit(self, X):
        if not self.q:
            self.q = X.shape[1]

        sc = StandardScaler()
        X = sc.fit_transform(X)

        pca = PCA(n_components=self.q).fit(X)
        A_q = pca.components_.T

        kmeans = KMeans(n_clusters=self.n_features).fit(A_q)
        clusters = kmeans.predict(A_q)
        cluster_centers = kmeans.cluster_centers_

        dists = defaultdict(list)
        for i, c in enumerate(clusters):
            dist = euclidean_distances([A_q[i, :]], [cluster_centers[c, :]])[0][0]
            dists[c].append((i, dist))

        self.indices_ = [sorted(f, key=lambda x: x[1])[0][0] for f in dists.values()]
        self.features_ = X[:, self.indices_]

class PCAFeatures(object):
    def __init__(self, n_features, q=None):
        self.q = q
        self.n_features = n_features
        self.feature_scores = []

    def fit(self, X):
        if not self.q:
            self.q = X.shape[1]

        sc = StandardScaler()
        X = sc.fit_transform(X)

        pca = PCA(n_components=self.q)
        X_reduced = pca.fit(X)
        components = X_reduced.components_.T
        self.feature_scores = [0] * len(X_reduced.explained_variance_ratio_)
        for i in range(0, len(X_reduced.explained_variance_ratio_)):
            pc_variance = X_reduced.explained_variance_ratio_[i]
            component_columns = components[i]
            for j in range(0, len(component_columns)):
                pc_feature_score = math.fabs(pc_variance * component_columns[j])
                self.feature_scores[j] += pc_feature_score

        #
        # pca.fit(X)
        # X_proj = pca.transform(X)
        # components = pca.components_
        # X_rec = pca.inverse_transform(X_proj)
        # print()



        # self.indices_ = [sorted(f, key=lambda x: x[1])[0][0] for f in dists.values()]
        # self.features_ = X[:, self.indices_]