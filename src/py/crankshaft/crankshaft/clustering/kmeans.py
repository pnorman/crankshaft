
def kmeans_1d(vals, n_bins):
    """
        Calculates the approximate bin edges for classifying the values into
        n_bins.
    """
    import numpy as np
    from sklearn.cluster import KMeans

    np.random.seed(5)

    X = np.array(vals)[:, np.newaxis]

    est = KMeans(n_clusters=n_bins)

    est.fit(X)
    centers = est.cluster_centers_
    centers.shape = (len(centers),)
    edges = np.append((centers[1:] + centers[0:-1]) / 2.0, np.max(vals))
    edges.sort()

    return edges
