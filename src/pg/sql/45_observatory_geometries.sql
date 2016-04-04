
-- Returns the polygon(s) that overlap with the input geometry.
-- Input:
-- :param geom geometry: input geometry
-- :param geometry_level text: table to get polygon from (can be approximate name)
-- :param use_literal boolean: use the literal table name (defaults to true)

-- From an input geometry (e.g., point), find the boundary roughly requested

CREATE OR REPLACE FUNCTION OBS_Get_Geometry(
  geom geometry,
  geometry_level text DEFAULT 'observatory.obs_a6b7e2e5de1ba72555fa54c8bf3ecc717d6161b4',
  use_literal boolean DEFAULT true,
  time_span text DEFAULT '2009 - 2013'
)
RETURNS geometry
AS $$
DECLARE
  boundary geometry;
  target_table text;
BEGIN

  IF use_literal
  THEN
    target_table := geometry_level;
  ELSE
    target_table := OBS_Search_Tables(geometry_level::text);
    RAISE NOTICE 'target_table: %', target_table;
  END IF;

  EXECUTE format(
    'SELECT t.the_geom
    FROM observatory.%s As t
    WHERE ST_Intersects($1, t.the_geom)
    LIMIT 1', target_table)
  INTO boundary
  USING geom;

  RETURN boundary;

END;
$$ LANGUAGE plpgsql;

-- From text address (e.g., '201 Moore St., Brooklyn, NY 11206'), give back the --  bounding geometry for the requested feature (e.g., 'census blocks', 'dma', 'blocks', etc.)

CREATE OR REPLACE FUNCTION OBS_Get_Geometry(
  address text,
  geometry_level text DEFAULT 'observatory.obs_a6b7e2e5de1ba72555fa54c8bf3ecc717d6161b4',
  use_literal boolean DEFAULT true,
  time_span text DEFAULT '2009 - 2013'
)
RETURNS geometry
AS $$
DECLARE
  boundary geometry;
  address_geom geometry;
  target_table text;
BEGIN

  IF use_literal
  THEN
    target_table := geometry_level;
  ELSE
    target_table := OBS_Search_Tables(geometry_level::text);
    RAISE NOTICE 'target_table: %', target_table;
  END IF;

  address_geom = cdb_geocode_street_point(address);

  EXECUTE format(
    'SELECT t.the_geom
    FROM observatory.%s As t
    WHERE ST_Intersects($1, t.the_geom)
    LIMIT 1', target_table)
  INTO boundary
  USING address_geom;

  RETURN boundary;

END;
$$ LANGUAGE plpgsql;
