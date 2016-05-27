CREATE OR REPLACE FUNCTION CDB_KMeans1D(vals NUMERIC[], n_bins INT)
RETURNS NUMERIC[]
AS $$

plpy.execute('SELECT cdb_crankshaft._cdb_crankshaft_activate_py()')
from crankshaft.clustering import kmeans_1d

return kmeans_1d(vals, n_bins)

$$ LANGUAGE plpythonu;
