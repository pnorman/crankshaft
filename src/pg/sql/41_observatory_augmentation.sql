
--For Longer term Dev


--Break out table definitions to types
--Automate type creation from a script, something like
----CREATE OR REPLACE FUNCTION OBS_Get<%=tag_name%>(geom GEOMETRY)
----RETURNS TABLE(
----<%=get_dimensions_for_tag(tag_name)%>
----AS $$
----DECLARE
----target_cols text[];
----names text[];
----vals numeric[];-
----q text;
----BEGIN
----target_cols := Array[<%=get_dimensions_for_tag(tag_name)%>],


--Functions for augmenting specific tables
--------------------------------------------------------------------------------

-- Creates a table of demographic snapshot

CREATE OR REPLACE FUNCTION OBS_Get_Demographic_Snapshot(address text )
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

  RETURN QUERY
    EXECUTE
    $query$
      SELECT * from OBS_Get_Demographic_Snapshot(cdb_geocode_street_point($1));
    $query$
    USING address;
  RETURN;
END
$$ LANGUAGE plpgsql


CREATE OR REPLACE FUNCTION OBS_Get_Demographic_Snapshot(geom GEOMETRY)
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

  RAISE NOTICE 'starting: %', timeofday();

  q = OBS_BUILD_SNAPSHOT_QUERY(target_cols);
  q = 'with a as (select colnames as names, colvalues as vals from OBS_AUGMENT_CENSUS($1,$2))' || q || ' from  a';
  RAISE NOTICE 'Ending : %', timeofday();

  RAISE NOTICE  "query: %", q
  RETURN QUERY
  EXECUTE
  q
  using geom, target_cols
  RETURN;
END
$$ LANGUAGE plpgsql


--Creates a table with the young family segment.

CREATE OR REPLACE FUNCTION OBS_AUGMENT_FAMILIES_WITH_YOUNG_CHILDREN_SEGMENT(address text)
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
BEGIN
RETURN QUERY
  EXECUTE
  $query$
    SELECT * from OBS_AUGMENT_FAMILIES_WITH_YOUNG_CHILDREN_SEGMENT(cdb_geocode_street_point($1));
  $query$
  USING address;
RETURN;
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

--Base functions for performing augmentation
----------------------------------------------------------------------------------------

--Creates a array based report of the named segment given a point or geometry
CREATE OR REPLACE FUNCTION OBS_AUGMENT_SEGMENT(
  geom geometry,
  segment_name text,
  time_span text DEFAULT '2009 - 2013',
  geometry_level text DEFAULT '"us.census.tiger".block_group')
RETURNS TABLE ( column_names text[], column_vals numeric[])
AS $$
BEGIN
  RETURN QUERY
  EXECUTE
  $query$
    SELECT * from OBS_AUGMENT_SEGMENT(cdb_geocode_street_point($1), $2, $3, $4);
  $query$
  USING
  address,
  segemnt_name,
  time_span,
  geometry_level;
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


--Grabs the value of a census dimension for given a point or geometry.

CREATE OR REPLACE FUNCTION OBS_Augment_Census(
  address text,
  dimension_name text,
  time_span text DEFAULT '2009 - 2013',
  geometry_level text DEFAULT '"us.census.tiger".block_group'
  )
RETURNS numeric as $$

BEGIN
  RETURN QUERY
  EXECUTE
  $query$
    SELECT * from OBS_AUGMENT_CENSUS(cdb_geocode_street_point($1), $2, $3, $4);
  $query$
  USING
  address,
  dimension_name,
  time_span,
  geometry_level;
  RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION OBS_Augment_Census(
  address text,
  dimension_names text[],
  time_span text DEFAULT '2009 - 2013',
  geometry_level text DEFAULT '"us.census.tiger".block_group'
  )
RETURNS TABLE( colnames text[], colvalues numeric[]) as $$

BEGIN
  RETURN QUERY
  EXECUTE
  $query$
    SELECT * from OBS_AUGMENT_CENSUS(cdb_geocode_street_point($1), $2, $3, $4);
  $query$
  USING
  address,
  dimension_names,
  time_span,
  geometry_level;
  RETURN;
END;
$$ LANGUAGE plpgsql;

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


--Returns arrays of values for the given census dimension names for a given
--point or polygon
CREATE OR REPLACE FUNCTION OBS_AUGMENT_CENSUS(
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

  ids  = OBS_LOOKUP_CENSUS_HUMAN(dimension_names);
  return query(select * from OBS_AUGMENT(geom, ids,time_span,geometry_level));

END
$$ LANGUAGE plpgsql ;

--Augments the target table with the desired census variable.
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


-- Base augmentation fucntion.
CREATE OR REPLACE FUNCTION OBS_AUGMENT(
  geom geometry,
  column_ids text[],
  time_span text,
  geometry_level text
)
RETURNS TABLE(names text[], vals NUMERIC[] )
AS $$
DECLARE
	results numeric[];
  geom_table_name text;
  names         text[];
  data_table_info OBS_COLUMN_DATA[];
BEGIN

  geom_table_name := OBS_GEOM_TABLE(geom,geometry_level);

  if geom_table_name is null then
     RAISE EXCEPTION 'Point % is outside of the data region.', geom;
  end if;

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
  geoid text;
  area  numeric;
BEGIN

  EXECUTE
    format('select geoid from observatory.%I where  the_geom && $1',  geom_table_name)
    using
    geom
    INTO geoid;

  EXECUTE
    format('select ST_AREA(the_geom::geography) from observatory.%I  where geoid = %L', geom_table_name, geoid)
    INTO
    area ;


  query = 'select Array[';
  FOR i in 1..array_upper(data_table_info,1)
  loop
    IF ((data_table_info)[i]).aggregate != 'sum' THEN
      query = query || format('%I ',((data_table_info)[i]).colname);
    else
      query = query || format('%I/%s ',
        ((data_table_info)[i]).colname,
        area);
    end if;
    IF i <  array_upper(data_table_info,1) THEN
      query = query || ',';
    end if;
  end loop;

  query = query || format(' ]
    from observatory.%I
    where %I.geoid  = %L
  ',
  ((data_table_info)[1]).tablename,
  ((data_table_info)[1]).tablename,
  geoid
  );


  EXECUTE  query  INTO result USING geom ;
  return result;

END
$$ LANGUAGE plpgsql;

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

    IF ((data_table_info)[i]).aggregate ='sum' then
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

  q = q || q_select || format('from observatory.%I ', ((data_table_info)[1].tablename)) ;

  q = q || ' ) ' || q_sum || ' ] from _overlaps, values
  where values.geoid  = _overlaps.geoid';

  execute q into result using geom;
  RETURN result;

END;
$$ LANGUAGE plpgsql;
