
-- Returns a list of avaliable geometry columns
CREATE OR REPLACE FUNCTION OBS_LIST_GEOM_COLUMNS() returns TABLE(column_id text) as $$
  SELECT id FROM observatory.bmd_column WHERE type ILIKE 'geometry';
$$
LANGUAGE SQL IMMUTABLE;

-- Returns the table name with geoms for the given geometry_id
-- TODO probably needs to take in the column_id array to get the relevant
-- table where there is multiple sources for a column from multiple
-- geometries.
CREATE OR REPLACE FUNCTION OBS_GEOM_TABLE (
  geom geometry, geometry_id text
)
RETURNS TEXT as $$
DECLARE
  result text;
BEGIN
  EXECUTE '
    SELECT tablename FROM observatory.bmd_table
    WHERE id IN (
      SELECT table_id
      FROM observatory.bmd_table tab,
           observatory.bmd_column_table coltable,
           observatory.bmd_column col
      WHERE type ILIKE ''geometry''
        AND coltable.column_id = col.id
        AND coltable.table_id = tab.id
        AND col.id = $1
        AND ST_INTERSECTS($2, ST_SetSRID(bounds::box2d::geometry, 4326))
    )
    '
  USING geometry_id, geom
  INTO result;

  return result;

END
$$  LANGUAGE plpgsql;

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
    FROM observatory.bmd_column_to_column c2c, observatory.bmd_column_table t
    WHERE c2c.reltype = ''geom_ref''
      AND c2c.target_id = $1
      AND c2c.source_id = t.column_id
    )
 SELECT colname, tablename, aggregate
 FROM observatory.bmd_column c, observatory.bmd_column_table ct, observatory.bmd_table t
 WHERE c.id = ct.column_id
   AND t.id = ct.table_id
   AND c.id = $2
   AND t.timespan = $3
   AND t.id in (SELECT id FROM geomref)
 ' USING geometry_id, column_id, timespan
 INTO result;
 RETURN result;
END
$$ LANGUAGE plpgsql;


--Gets the column id for a census variable given a human readable version of it
CREATE OR REPLACE FUNCTION OBS_LOOKUP_CENSUS_HUMAN(
  column_name text,
  table_name text DEFAULT '"us.census.acs".extract_year_2013_sample_5yr_geography_block_group'
)
RETURNS text as $$
DECLARE
  column_id text;
  result text;
BEGIN
    EXECUTE format('select column_id from observatory.bmd_column_table where colname = %L  and table_id = %L limit 1', column_name,table_name)
    INTO result;
    RETURN result;
END
$$ LANGUAGE plpgsql;


--Test point cause Stuart always seems to make random points in the water
CREATE OR REPLACE FUNCTION _TEST_POINT()
RETURNS geometry
AS $$
BEGIN
  return CDB_latlng(40.704512,-73.936669);
END
$$ LANGUAGE plpgsql

--Test polygon cause Stuart always seems to make random points in the water
CREATE OR REPLACE FUNCTION _TEST_AREA()
RETURNS geometry
AS $$
BEGIN
  return ST_BUFFER(_TEST_POINT()::geography, 500)::geometry;
END
$$ LANGUAGE plpgsql

--Used to expand a column based response to a table based one. Give it the desired
--columns and it will return a partial query for rolling them out to a table.
CREATE OR REPLACE FUNCTION OBS_BUILD_SNAPSHOT_QUERY(names text[])
RETURNS TEXT AS $$
DECLARE
  q text;
  i numeric;
BEGIN
  q ='select ' ;
  for i in 1..array_upper(names,1)
  loop
    q = q || format(' vals[%I] %I', i, names[i]);
    if i<array_upper(names,1) then
      q= q||',';
    end if;
  end loop;
  return q;
END
$$ LANGUAGE plpgsql;

--Type for describing column data from the Data Obseravtory
CREATE TYPE OBS_COLUMN_DATA as(colname text , tablename text ,aggregate text);