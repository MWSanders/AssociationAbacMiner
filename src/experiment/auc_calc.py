import numpy as np
from sklearn import metrics
# fpr = np.array([0.0, 0.074095, 0.075182,0.078711,0.082167,0.083535,0.083821,0.092029,0.411606, 1.0])

# tpr = np.array([0.0, 0.965546,0.974578,0.969905,0.975123,0.974718,0.974698,0.977512,0.992252, 1.0])


fpr = np.array([0.0, 0.075182, 1.0])
tpr = np.array([0.0, 0.974578, 1.0])
mean_auc = metrics.auc(fpr, tpr)
print(mean_auc)

fpr = np.array([0.0, 0.655218, 1.0])
tpr = np.array([0.0, 0.998326, 1.0])
mean_auc = metrics.auc(fpr, tpr)
print(mean_auc)