
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

CREATE OR REPLACE FUNCTION OBS_SEARCH(
  search_term text
)
RETURNS TABLE(description text, name text, aggregate text,source text )  as $$
BEGIN
  RETURN QUERY
  EXECUTE format($string$
              SELECT description,
                name,
                  aggregate,
                  replace(split_part(id,'".', 1),'"', '') source
                  FROM observatory.bmd_column
                  where name ilike '%%%L%%'
                  or description ilike '%%%L%%'
                $string$, search_term, search_term);
  RETURN;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION OBS_LIST_DIMENSIONS_FOR_TABLE(table_name text )
RETURNS TABLE(colname text) AS $$
BEGIN
  RETURN QUERY
    EXECUTE format('select colname from observatory.bmd_column_table  where table_id = %L ', table_name);
  RETURN;
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


CREATE OR REPLACE FUNCTION OBS_DESCRIBE_SEGMENT(segment_name text)
returns TABLE(
  id text,
  name text,
  description text,
  column_names text[],
  column_descriptions text[],
  column_ids text[],
  example_function_call text
)as $$
BEGIN
  RETURN QUERY
  EXECUTE
    format(
      $query$
        SELECT bmd_tag.id, bmd_tag.name, bmd_tag.description,
        array_agg(bmd_column.name) column_names,
        array_agg(bmd_column.description) column_descriptions,
        array_agg(bmd_column.id) column_ids,
        'select OBS_AUGMENT_SEGMENT(the_geom,''' || bmd_tag.id || ''')' example_function_call
        FROM bmd_tag, bmd_column_tag, bmd_column
        where bmd_tag.id = bmd_column_tag.tag_id
        and bmd_column.id = bmd_column_tag.column_id
        and bmd_tag.id ilike '%%%s%%'
        group by bmd_tag.name, bmd_tag.description, bmd_tag.id
      $query$, segment_name);
  RETURN;
END $$ LANGUAGE plpgsql

CREATE OR REPLACE FUNCTION OBS_LIST_AVAILABLE_SEGMENTS()
returns TABLE(
  id text,
  name text,
  description text,
  column_names text[],
  column_descriptions text[],
  column_ids text[],
  example_function_call text
) as $$
BEGIN
  RETURN QUERY
    EXECUTE
      $query$
      SELECT bmd_tag.id, bmd_tag.name, bmd_tag.description,
      array_agg(bmd_column.name) column_names,
      array_agg(bmd_column.description) column_descriptions,
      array_agg(bmd_column.id) column_ids,
      'select OBS_AUGMENT_SEGMENT(the_geom,''' || bmd_tag.id || ''')' example_function_call
      FROM bmd_tag, bmd_column_tag, bmd_column
      where bmd_tag.id = bmd_column_tag.tag_id
      and bmd_column.id = bmd_column_tag.column_id
      group by bmd_tag.name, bmd_tag.description, bmd_tag.id
      $query$
  RETURN;
END
$$ LANGUAGE plpgsql

CREATE OR REPLACE FUNCTION _TEST_POINT()
RETURNS geometry
AS $$
BEGIN
  return CDB_latlng(40.704512,-73.936669);
END
$$ LANGUAGE plpgsql

CREATE OR REPLACE FUNCTION _TEST_AREA()
RETURNS geometry
AS $$
BEGIN
  return ST_BUFFER(_TEST_POINT()::geography, 500)::geometry;
END
$$ LANGUAGE plpgsql

CREATE OR REPLACE FUNCTION OBS_AUGMENT_FAMILIES_WITH_YOUNG_CHILDREN_SEGMENT(the_geom geometry)
RETURNS TABLE (
  geom geometry,
  families_with_young_children Numeric,
  two_parent_families_with_young_children Numeric,
  two_parents_in_labor_force_families_with_young_children Numeric,
  two_parents_father_in_labor_force_families_with_young_children Numeric,
  two_parents_mother_in_labor_force_families_with_young_children Numeric,
  two_parents_not_in_labor_force_families_with_young_children Numeric,
  one_parent_families_with_young_children NUMERIC,
  father_one_parent_families_with_young_children Numeric
)
AS $$
DECLARE
  column_ids text[];
  q text;
  segment_name text;
BEGIN

  segment_name = '"us.census.segments".families_with_young_children';

  EXECUTE
    $query$
    select column_ids from OBS_DESCRIBE_SEGMENT($1) limit 1;
    $query$
  INTO column_ids
  USING
  segment_name;

  q = OBS_BUILD_SNAPSHOT_QUERY(column_ids);
  q = 'with a as (select column_names as names, column_vals as vals from OBS_AUGMENT_SEGMENT($1,$2))' || q || ' from  a';
  RETURN QUERY
  EXECUTE
  q
  using the_geom, segment_name
  RETURN;
END
$$ LANGUAGE plpgsql


CREATE OR REPLACE FUNCTION OBS_AUGMENT_SEGMENT(
  geom geometry,
  segment_name text,
    time_span text DEFAULT '2009 - 2013',
  geometry_level text DEFAULT '"us.census.tiger".block_group')
RETURNS TABLE ( column_names text[], column_vals numeric[])
AS $$
DECLARE
 column_ids text[];
BEGIN

  EXECUTE
    $query$
    select column_ids from OBS_DESCRIBE_SEGMENT($1) limit 1;
    $query$
  INTO column_ids
  USING
  segment_name;

  if column_ids is null  then
    RAISE 'Could not find a segment %', segment_name;
  end if;

  RETURN QUERY
    EXECUTE
    $query$
      select  * from OBS_AUGMENT( $1, $2, $3, $4);
    $query$
    USING geom, column_ids, time_span, geometry_level
  RETURN;
END
$$ LANGUAGE plpgsql


CREATE OR REPLACE FUNCTION OBS_Augment_Census(
  geom geometry,
  dimension_name text,
  time_span text DEFAULT '2009 - 2013',
  geometry_level text DEFAULT '"us.census.tiger".block_group'
  )
RETURNS numeric as $$
DECLARE
  column_id text;
BEGIN
  column_id = OBS_LOOKUP_CENSUS_HUMAN(dimension_name);
  if column_id is null then
    RAISE EXCEPTION 'Column % does not exist ', dimension_name;
  end if;
  return OBS_AUGMENT(geom, ARRAY[column_id], time_span, geometry_level);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION OBS_AUGMENT_CENSUS_MUTLI(
  geom geometry,
  dimension_names text[],
  time_span text DEFAULT '2009 - 2013',
  geometry_level text DEFAULT '"us.census.tiger".block_group'
)
RETURNS TABLE( colnames text[], colvalues numeric[])
AS $$
DECLARE
  ids text[];
BEGIN
  ids = (
    WITH b as(
      select OBS_LOOKUP_CENSUS_HUMAN(unnest(dimension_names)) a
    )
    select array_agg(b.a) from b
  );
  return query(select * from OBS_AUGMENT(geom, ids,time_span,geometry_level));

END
$$ LANGUAGE plpgsql ;



CREATE OR REPLACE FUNCTION OBS_Augment_Census(
  geom geometry,
  dimension_name text,
  time_span text DEFAULT '2009 - 2013',
  geometry_level text DEFAULT '"us.census.tiger".block_group'
  )
RETURNS numeric as $$
DECLARE
  column_id text;
BEGIN
  column_id = OBS_LOOKUP_CENSUS_HUMAN(dimension_name);
  if column_id is null then
    RAISE EXCEPTION 'Column % does not exist ', dimension_name;
  end if;
  return OBS_AUGMENT(geom, column_id, time_span, geometry_level);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION OBS_AUGMENT_TABLE_WITH_CENSUS(
  table_name text,
  dimension_name text
) RETURNS VOID AS $$
BEGIN
  BEGIN

    EXECUTE format('ALTER TABLE %I add column %I NUMERIC', table_name,dimension_name);
  EXCEPTION
    WHEN duplicate_column then
      RAISE NOTICE 'Column does not exist';
    END;

  EXECUTE format('UPDATE %I
    SET %I = v.%I
    FROM (
      select cartodb_id, OBS_Augment_Census(the_geom, %L) as %I
      from %I
    ) v
    WHERE v.cartodb_id= %I.cartodb_id;
  ', table_name, dimension_name,dimension_name,dimension_name,dimension_name,table_name,table_name);

END;
$$ LANGUAGE plpgsql ;


CREATE OR REPLACE FUNCTION OBS_AUGMENT(
  geom geometry,
  column_ids text[],
  time_span text,
  geometry_level text
)
RETURNS TABLE(names text[], vals Numeric[])
AS $$
DECLARE
	results numeric[];
  geom_table_name text;
  names   text[];
  data_table_info OBS_COLUMN_DATA[];
BEGIN

  geom_table_name := OBS_GEOM_TABLE(geometry_level);

  data_table_info := (
    with ids as( select unnest(column_ids) id)
    select array_agg(OBS_GET_COLUMN_DATA(geometry_level, ids.id, time_span))
    from ids
  );

  names  = (select array_agg((d).colname) from unnest(data_table_info) as  d );

  IF ST_GeometryType(geom) = 'ST_Point' then
    results  = OBS_AUGMENT_POINTS(geom, geom_table_name, data_table_info);
  ELSIF ST_GeometryType(geom) in ('ST_Polygon', 'ST_MultiPolygon') then
    results  = OBS_AUGMENT_POLYGONS(geom,geom_table_name, data_table_info);
  end if;

  if results is null then
    results= Array[];
  end if;

  return query (select  names, results) ;
END;
$$  LANGUAGE plpgsql

-- OBS_AUGMENT takes a target geometry, the column_id that we want to augment with, the time span and a geometry level
CREATE OR REPLACE FUNCTION OBS_AUGMENT_ONE(
  geom geometry,
  column_id[] text,
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
  data_table_info OBS_COLUMN_DATA[]
) RETURNS Numeric[] AS $$
DECLARE
  result Numeric[];
  query  text;
  i Numeric;
BEGIN

  query = 'select Array[';
  FOR i in 1..array_upper(data_table_info,1)
  loop
    IF ((data_table_info)[i]).aggregate != 'sum' THEN
      query = query || format('%I ',((data_table_info)[i]).colname);
    else
      query = query || format('%I/ST_AREA(%I.the_geom::geography) ',
        ((data_table_info)[i]).colname,
        geom_table_name);
    end if;
    IF i <  array_upper(data_table_info,1) THEN
      query = query || ',';
    end if;
  end loop;

  query = query || format(' ]
    from observatory.%I, observatory.%I
    where substr(%I.geoid , 8) = %I.geoid
    and  %I.the_geom && $1
  ',
  ((data_table_info)[1]).tablename,
  geom_table_name,
  ((data_table_info)[1]).tablename,
  geom_table_name,
  geom_table_name);

  EXECUTE  query  INTO result USING geom ;
  return result;

END
$$ LANGUAGE plpgsql;


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


CREATE TYPE OBS_COLUMN_DATA as(colname text , tablename text ,aggregate text);

CREATE TYPE OBS_CENSUS_SUMMARY as(
"Gini Index" Numeric,
"Vacant Housing Units for Rent" Numeric,
"Vacant Housing Units for Sale" Numeric,
"Commuters by Subway or Elevated" Numeric,
"Commuters by Public Transportation" Numeric,
"Commuters by Bus" Numeric,
"Workers over the Age of 16" Numeric,
"Commuters by Car, Truck, or Van" Numeric,
"Walked to Work" Numeric,
"Worked at Home" Numeric,
"Students Enrolled in Grades 1 to 4" Numeric,
"Students Enrolled in School" Numeric,
"Students Enrolled in Grades 5 to 8" Numeric,
"Students Enrolled in Grades 9 to 12" Numeric,
"Students Enrolled as Undergraduate in College" Numeric,
"Population 3 Years and Over" Numeric,
"Vacant Housing Units" Numeric,
"Housing Units" Numeric,
"Owner-occupied Housing Units" Numeric,
"Owner-occupied Housing Units valued at $1,000,000 or more." Numeric,
"Owner-occupied Housing Units with a Mortgage" Numeric,
"Median Age" Numeric,
"Percent of Household Income Spent on Rent" Numeric,
"Children under 18 Years of Age" Numeric,
"Population Completed Master's Degree" Numeric,
"Population 25 Years and Over" Numeric,
"Population Completed High School" Numeric,
"Population Completed Bachelor's Degree" Numeric,
"Households" Numeric,
"Total Population" Numeric,
"Male Population" Numeric,
"White Population" Numeric,
"Black or African American Population" Numeric,
"Asian Population" Numeric,
"Not a U.S. Citizen Population" Numeric,
"Speaks Spanish at Home" Numeric,
"Population 5 Years and Over" Numeric,
"Speaks only English at Home" Numeric,
"Per Capita Income in the past 12 Months" Numeric,
"Median Household Income in the past 12 Months" Numeric,
"Population for Whom Poverty Status Determined" Numeric,
"Median Rent" Numeric
);



CREATE OR REPLACE FUNCTION OBS_GetDemographicSnapshot(geom GEOMETRY)
RETURNS TABLE(
gini_index Numeric,
vacant_housing_units_for_rent Numeric,
vacant_housing_units_for_Sale Numeric,
commuters_by_Subway_or_Elevated Numeric,
commuters_by_Public_Transportation Numeric,
commuters_by_Bus Numeric,
workers_over_the_age_of_16 Numeric,
commuter_by_car_truck_or_van Numeric,
walked_to_work Numeric,
worked_at_Home Numeric,
students_enrolled_in_Grades_1_to_4 Numeric,
students_enrolled_in_School Numeric,
students_enrolled_in_Grades_5_to_8 Numeric,
students_enrolled_in_Grades_9_to_12 Numeric,
students_enrolled_as_Undergraduate_in_College Numeric,
population_3_years_and_over Numeric,
vacant_housing_units Numeric,
housing_units Numeric,
owner_occupied_Housing_Units Numeric,
owner_occupied_Housing_Units_valued_at_1_000_000_or_more Numeric,
owner_occupied_Housing_Units_with_a_Mortgage Numeric,
median_age Numeric,
percent_of_household_income_spent_on_rent Numeric,
children_under_18_years_of_age Numeric,
population_Completed_Masters_Degree Numeric,
population_25_Years_and_Over Numeric,
population_Completed_High_School Numeric,
population_Completed_Bachelors_Degree Numeric,
households Numeric,
total_population Numeric,
male_population Numeric,
white_population Numeric,
black_or_african_american_population Numeric,
asian_population Numeric,
not_a_US_Citizen_Population Numeric,
speaks_spanish_at_home Numeric,
population_5_years_and_over Numeric,
speaks_only_english_at_home Numeric,
per_capita_income_in_the_past_12_months Numeric,
median_Household_Income_in_the_past_12_Months Numeric,
population_for_Whom_Poverty_Status_Determined Numeric,
median_Rent Numeric)
AS $$
DECLARE
 target_cols text[];
 names text[];
 vals numeric[];
 q text;
 BEGIN
 target_cols := Array['gini_index',
 'vacant_housing_units_for_rent',
 'vacant_housing_units_for_sale',
 'commuters_by_subway_or_elevated',
 'commuters_by_public_transportation',
 'commuters_by_bus',
 'workers_16_and_over',
 'commuters_by_car_truck_van',
 'walked_to_work',
 'worked_at_home',
 'in_grades_1_to_4',
 'in_school',
 'in_grades_5_to_8',
 'in_grades_9_to_12',
 'in_undergrad_college',
 'population_3_years_over',
 'vacant_housing_units',
 'housing_units',
 'owner_occupied_housing_units',
 'million_dollar_housing_units',
 'mortgaged_housing_units',
 'median_age',
 'percent_income_spent_on_rent',
 'children',
 'masters_degree',
 'pop_25_years_over',
 'high_school_diploma',
 'bachelors_degree',
 'households',
 'total_pop',
 'male_pop',
 'white_pop',
 'black_pop',
 'asian_pop',
 'not_us_citizen_pop',
 'speak_spanish_at_home',
 'pop_5_years_over',
 'speak_only_english_at_home',
 'income_per_capita',
 'median_income',
 'pop_determined_poverty_status',
 'median_rent'];

  q = OBS_BUILD_SNAPSHOT_QUERY(target_cols);
  q = 'with a as (select colnames as names, colvalues as vals from OBS_AUGMENT_CENSUS_MUTLI($1,$2))' || q || ' from  a';
  RETURN QUERY
  EXECUTE
  q
  using geom, target_cols
  RETURN;
END
$$ LANGUAGE plpgsql




CREATE OR REPLACE FUNCTION OBS_AUGMENT_POLYGONS (
  geom geometry,
  geom_table_name text,
  data_table_info OBS_COLUMN_DATA[]
) returns numeric[] AS $$
DECLARE
  result numeric[];
  q_select text;
  q_sum text;
  q text;
  i numeric;
BEGIN


  q_select = 'select geoid, ';
  q_sum    = 'select Array[';

  FOR i IN 1..array_upper(data_table_info, 1) LOOP
    q_select = q_select || format( '%I ', ((data_table_info)[i]).colname);
    if ((data_table_info)[i]).aggregate ='sum' then
      q_sum    = q_sum || format('sum(overlap_fraction * COALESCE(%I,0)) ',((data_table_info)[i]).colname,((data_table_info)[i]).colname);
    else
      q_sum    = q_sum || ' null ';
    end if;
    IF i < array_upper(data_table_info,1) THEN
      q_select = q_select || format(',');
      q_sum     = q_sum || format(',');
	end IF;
   end LOOP;

  q = format('
    WITH _overlaps AS(
      select  ST_AREA(ST_INTERSECTION($1, a.the_geom))/ST_AREA(a.the_geom) overlap_fraction, geoid
      from observatory.%I as a
      where $1 && a.the_geom
    ),
    values AS(
    ',geom_table_name );

  q = q || q_select || format('from %I ', ((data_table_info)[1].tablename)) ;

  q = q || ' ) ' || q_sum || ' ] from _overlaps, values
  where substr(values.geoid , 8) = _overlaps.geoid';



  execute q into result using geom;
  RETURN result;

END;
$$ LANGUAGE plpgsql;
