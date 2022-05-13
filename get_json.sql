


/*
FUNCTION: get_json(text, text, text, taxt)
	description :
		-> gets a json resource using curl. If it does not exists, creates a PG table destination_schema_name.destination_table_name, else populate it with new lines (id value not already in)
	
	parameters :
		url text 							-- the query defining the data
		destination_schema_name text		-- the query defining the columns
		destination_table_name text			-- the query defining the columns
		unique_json_attribute text			-- a unique attribute, useful to ignore lines already in the table
		
	returning :
		void
*/

CREATE OR REPLACE FUNCTION the_schema_you_want.get_json(
	url text,
	destination_schema_name text,
	destination_table_name text,
	unique_json_attribute text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare requete text;
BEGIN
EXECUTE (
		'DROP TABLE IF EXISTS json_data_from_web;
		 CREATE TEMP TABLE json_data_from_web(form_data json);'
		);
EXECUTE format('COPY json_data_from_web FROM PROGRAM ''curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 "'||url||'"'' CSV QUOTE E''\x01'' DELIMITER E''\x02'';');
EXECUTE format('CREATE TABLE IF NOT EXISTS '||destination_schema_name||'.'||destination_table_name||' (form_data json);');

EXECUTE format ('CREATE UNIQUE INDEX IF NOT EXISTS '||destination_table_name||'_id_idx
    ON '||destination_schema_name||'.'||destination_table_name||' USING btree
    ((form_data ->> '''||unique_json_attribute||'''::text) COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;');

EXECUTE format('INSERT into '||destination_schema_name||'.'||destination_table_name||'(form_data) SELECT json_array_elements(form_data -> ''data'') AS form_data 
			   FROM json_data_from_web 
			   ON CONFLICT ((form_data ->> '''||unique_json_attribute||'''::text)) DO NOTHING
			   ;');
END;
$BODY$;

COMMENT ON FUNCTION the_schema_you_want.get_json(text,text,text,taxt) IS 'description :
		-> gets a json resource using curl. If it does not exists, creates a PG table destination_schema_name.destination_table_name, else populate it with new lines (id value not already in)
	
	parameters :
		url text 							-- the query defining the data
		destination_schema_name text		-- the query defining the columns
		destination_table_name text			-- the query defining the columns
		unique_json_attribute text			-- a unique attribute, useful to ignore lines already in the table
		
	returning :
		void';