 
{% macro create_new_diff_table(target_table, target_schema) %}

  SELECT 
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(ddl, 'BOOL', 'STRING'), 'FLOAT64', 'STRING'),'INT64','STRING'),'' || target_table || '_non_string_schema','' || target_table || '_new_differential'),'DATETIME','STRING'),'DATE','STRING'),'TIMESTAMP','STRING') AS new_ddl
    ,ROW_NUMBER() OVER ()
 FROM `sandbox-444621.region-us.INFORMATION_SCHEMA.TABLES`
WHERE table_name = target_table ||'_non_string_schema'
AND table_schema = target_schema

{% endmacro %}