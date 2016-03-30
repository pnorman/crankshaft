--Functions use to search the observatroy for information
--------------------------------------------------------------------------------
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

--Fuctions used to describe and search segments
--------------------------------------------------------------------------------
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