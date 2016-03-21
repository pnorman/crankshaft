-- Returns a list of avaliable geometry columns
CREATE OR REPLACE FUNCTION OBS_LIST_GEOM_COLUMNS() returns TABLE(column_id text) as $$
  SELECT id FROM observatory.bmd_column WHERE type ILIKE 'geometry';
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
    SELECT tablename FROM observatory.bmd_table
    WHERE id IN (
      SELECT table_id
      FROM observatory.bmd_column_table coltable, observatory.bmd_column col
      WHERE type ILIKE ''geometry''
        AND coltable.column_id = col.id
        AND col.id = $1
    )
    '
  USING geometry_id
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
      AND c2c.source_id = $1
      AND c2c.target_id = t.column_id
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

CREATE OR REPLACE FUNCTION OBS_GET_CENSUS_VARIABLE(
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
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION OBS_AUGMENT_WITH_CENSUS_VARIABLE(
  table_name text,
  variable_name text
) RETURNS VOID AS $$
BEGIN
  BEGIN

    EXECUTE format('ALTER TABLE %I add column %I NUMERIC', table_name,variable_name);
  EXCEPTION
    WHEN duplicate_column then
      RAISE NOTICE 'Column does not exist';
    END;



  EXECUTE format('UPDATE %I
    SET %I = v.%I
    FROM (
      select cartodb_id, OBS_GET_CENSUS_VARIABLE(the_geom, %L) as %I
      from %I
    ) v
    WHERE v.cartodb_id= %I.cartodb_id;
  ', table_name, variable_name,variable_name,variable_name,variable_name,table_name,table_name);

END;
$$ LANGUAGE plpgsql ;


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

  IF ST_GeometryType(geom) = 'ST_Point' then
    result  = OBS_AUGMENT_POINTS(geom, geom_table_name, data_table_info);
  ELSIF ST_GeometryType(geom) in ('ST_Polygon', 'ST_MultiPolygon') then
    result  = OBS_AUGMENT_POLYGONS(geom, geom_table_name, data_table_info);
  end if;

  if result is null then
    result= 0;
  end if;

  return result;
END;
$$  LANGUAGE plpgsql;

-- IF the variable of interest is just a rate return it as such, othewise normalize
-- it to the census block area and return that
CREATE OR REPLACE FUNCTION OBS_AUGMENT_POINTS(
  geom geometry,
  geom_table_name text,
  data_table_info OBS_COLUMN_DATA
) RETURNS NUMERIC AS $$
DECLARE
  result Numeric;
  query  text;
BEGIN

  if data_table_info.aggregate != 'sum' then
    query = format('
      select %I
      from observatory.%I, observatory.%I
      where substr(%I.geoid , 8) = %I.geoid
      and  %I.the_geom && $1
      ' ,
      data_table_info.colname,
      data_table_info.tablename,
      geom_table_name,
      data_table_info.tablename,
      geom_table_name,
      geom_table_name);
  else
    query = format('
      select %I/ST_AREA(%I.the_geom::geography)
      from observatory.%I, observatory.%I
      where substr(%I.geoid , 8) = %I.geoid
      and  %I.the_geom && $1
      ',
      data_table_info.colname,
      geom_table_name,
      data_table_info.tablename,
      geom_table_name,
      data_table_info.tablename,
      geom_table_name,
      geom_table_name);

  end if;


  EXECUTE query
    USING geom
    INTO result;

  RETURN result;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION OBS_AUGMENT_POLYGONS (
  geom geometry,
  geom_table_name text,
  data_table_info OBS_COLUMN_DATA
) RETURNS NUMERIC AS $$
DECLARE
  result numeric;
BEGIN

  IF data_table_info.aggregate != 'sum' THEN
    RAISE EXCEPTION 'Target column is not a sum value, cant aggregate!'
          USING HINT = 'Pick a column which is a sum or a count';
  end if;


  EXECUTE format('
    WITH _overlaps AS(
      select  ST_AREA(ST_INTERSECTION($1, a.the_geom))/ST_AREA(a.the_geom) overlap_fraction, geoid
      from observatory.%I as a
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
$$ LANGUAGE plpgsql;
