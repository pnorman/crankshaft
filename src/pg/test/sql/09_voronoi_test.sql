WITH a AS (
    SELECT
    ARRAY[ST_GeomFromText('POINT(2.1744 41.403)'),ST_GeomFromText('POINT(2.1228 41.380)'),ST_GeomFromText('POINT(2.1511 41.374)'),ST_GeomFromText('POINT(2.1528 41.413)'),ST_GeomFromText('POINT(2.165 41.391)'),ST_GeomFromText('POINT(2.1498 41.371)'),ST_GeomFromText('POINT(2.1533 41.368)'),ST_GeomFromText('POINT(2.131386 41.41399)')] AS geomin
)
SELECT
    CDB_voronoi(geomin, 0.2, 1e-9) as result
FROM a;
