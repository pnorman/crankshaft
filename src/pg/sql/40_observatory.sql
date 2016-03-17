-- Returns a list of avaliable geometry columns
CREATE OR REPLACE FUNCTION OBS_LIST_GEOM_COLUMNS() returns TABLE(column_id text) as $$
  SELECT id FROM bmd_column WHERE type ILIKE 'geometry';
$$
LANGUAGE SQL IMMUTABLE;

-- Returns the table name with geoms for the given geometry_id
CREATE OR REPLACE FUNCTION OBS_GEOM_TABLE (
  geometry_id text
)
RETURNS TEXT as $$
DECLARE
  result text;
BEGIN
  EXECUTE '
    SELECT tablename FROM bmd_table
    WHERE id IN (
      SELECT table_id
      FROM bmd_column_table coltable, bmd_column col
      WHERE type ILIKE ''geometry''
        AND coltable.column_id = col.id
        AND col.id = $1
    )
    '
  USING geometry_id
  INTO result;

  return result;

END
$$  LANGUAGE plpgsql

-- A type for use with the OBS_GET_COLUMN_DATA function
CREATE TYPE OBS_COLUMN_DATA as(colname text , tablename text ,aggregate text);


-- A function that gets teh column data for a column_id, geometry_id and timespan.
CREATE OR REPLACE FUNCTION OBS_GET_COLUMN_DATA(
geometry_id text, column_id text, timespan text)
RETURNS OBS_COLUMN_DATA as $$
DECLARE
RESULT OBS_COLUMN_DATA;
BEGIN
  EXECUTE '
  WITH geomref AS (
    SELECT t.table_id id
    FROM bmd_column_to_column c2c, bmd_column_table t
    WHERE c2c.reltype = ''geom_ref''
      AND c2c.source_id = $1
      AND c2c.target_id = t.column_id
  )
  SELECT colname, tablename, aggregate
  FROM bmd_column c, bmd_column_table ct, bmd_table t
  WHERE c.id = ct.column_id
    AND t.id = ct.table_id
    AND c.id = $2
    AND t.timespan = $3
    AND t.id in (SELECT id FROM geomref)
  ' USING geometry_id, column_id, timespan
  INTO result;
  RETURN result;
END
$$ LANGUAGE plpgsql


CREATE OR REPLACE FUNCTION OBS_LOOKUP_CENSUS_HUMAN(
  column_name text,
  table_name text DEFAULT '"us.census.acs".extract_year_2013_sample_5yr_geography_block_group'
)
RETURNS text as $$
DECLARE
  column_id text;
  result text;
BEGIN
    EXECUTE format('select column_id from bmd_column_table where colname = %L  and table_id = %L limit 1', column_name,table_name)
    INTO result;
    RETURN result;
END
$$ LANGUAGE plpgsql

CREATE OR REPLACE FUNCTION OBS_AUGMENT_CENSUS(
  geom geometry,
  column_name text,
  time_span text DEFAULT '2009 - 2013',
  geometry_level text DEFAULT '"us.census.tiger".block_group'
  )
RETURNS numeric as $$
DECLARE
  column_id text;
BEGIN
  column_id = OBS_LOOKUP_CENSUS_HUMAN(column_name);
  if column_id is null then
    RAISE EXCEPTION 'Column does not exist'
        USING HINT = 'Try using OBS_CENSUS_COLUMN_LIST to get a list of avaliable data';
  end if;
  return OBS_AUGMENT(geom, column_id, time_span, geometry_level);
END;
$$ LANGUAGE plpgsql



-- OBS_AUGMENT takes a target geometry, the column_id that we want to augment with, the time span and a geometry level
CREATE OR REPLACE FUNCTION OBS_AUGMENT(
  geom geometry,
  column_id text,
  time_span text,
  geometry_level text
)
RETURNS numeric as $$
DECLARE
	result numeric;
    geom_table_name text;
    data_table_info OBS_COLUMN_DATA;
BEGIN


geom_table_name := OBS_GEOM_TABLE(geometry_level);
data_table_info := OBS_GET_COLUMN_DATA(geometry_level, column_id, time_span);

IF data_table_info.aggregate != 'sum' THEN
  RAISE EXCEPTION 'Target column is not a sum value, cant aggregate!'
        USING HINT = 'Pick a column which is a sum or a count';
end if;

EXECUTE format('
  WITH _overlaps AS(
    select  ST_AREA(ST_INTERSECTION($1, a.the_geom))/ST_AREA(a.the_geom) overlap_fraction, geoid
    from %I as a
    where $1 && a.the_geom
  ),
  values AS(
    select geoid, %I as val from %I
  )

  select sum(overlap_fraction * COALESCE(val, 0))
  from _overlaps, values
  where substr(values.geoid , 8) = _overlaps.geoid
  ' , geom_table_name,data_table_info.colname, data_table_info.tablename )
USING geom
INTO result;

RETURN result;
END;

$$  LANGUAGE plpgsql
