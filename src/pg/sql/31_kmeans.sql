CREATE OR REPLACE FUNCTION CDB_KMeans1D(vals NUMERIC[], n_bins INT)
RETURNS NUMERIC[]
AS $$

import numpy as np
from sklearn.cluster import KMeans

np.random.seed(5)

X = np.array(vals)
X = X[:, np.newaxis]

est = KMeans(n_clusters=n_bins)

est.fit(X)
centers = np.array([v[0] for v in (est.cluster_centers_).tolist()])
edges = np.append((centers[1:] - centers[0:-1]) / 2.0, np.max(vals))
edges.sort()

return edges

$$ LANGUAGE plpythonu;
