 {% macro create_new_differential_table(target_table, target_schema, drop_existing_object=False, project=target.project, dry_run=True) %}
    
    {# Drop existing new_differential table #}
    {% if drop_existing_object %}
        {{ log('Dropping Existing Objects:.', info=True) }}
        {% set drop_existing_object_query = "DROP TABLE IF EXISTS " ~ project ~ "." ~ target_schema ~ "." ~ target_table ~ "_new_differential" %}
        {{ log('Executing query: ' ~ drop_existing_object_query, info=True) }}
        {% do run_query(drop_existing_object_query) %} 
    {% endif %}

    {% set get_create_commands_query %}
    SELECT 
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(ddl,
                             'BOOL', 'STRING')
                            , 'FLOAT64', 'STRING')
                        ,'INT64','STRING')
                    ,'{{ target_table ~ '_non_string_schema'}}','{{ target_table  ~ '_new_differential'}}')
                ,'DATETIME','STRING')
            ,'DATE','STRING')
        ,'TIMESTAMP','STRING') AS new_ddl
    FROM `{{ project }}.region-us.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = '{{ target_table ~ '_non_string_schema'}}' 
    AND upper(table_schema) = upper('{{ target_schema }}')
    {% endset %}

    {{ log('Print Select command that fetches DDL from INFORMATION_SCHEMA.TABLES :' + get_create_commands_query, info=True)}}
    {% set create_queries = run_query(get_create_commands_query).columns[0].values() %}

    {#- Check if the non_string_schema object exists. If it doesn't no rows will return from the preceeding query. -#}
    {% if create_queries | length == 0 %}
        {% do log("There is no object called " ~ project ~ "." ~ target_schema ~ "." ~ target_table ~ "_non_string_schema in project " ~ project ~ ".", info=True) %}
        {% do log("Please re-run ingest to ensure an object is properly landed into the project " ~ project ~ " prior to data processing.", info=True) %}
        {{return('')}}  {#- Exit the macro -#}
    {% endif %}

    {% for query in create_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Creating object with command: ' ~ query, info=True) }}
            {% do run_query(query) %} 
        {% endif %}       
    {% endfor %}

    {% set next_macro_arg = "copy_non_string_schema_to_new_differential" %}
    {{ log('Attempting to execute next stage of the merging process: ' ~ next_macro_arg, info=True) }}
    {{ execute_next_merging_macro(next_macro=next_macro_arg, target_table=target_table, target_schema=target_schema, project=project, dry_run=dry_run) }}
    
{% endmacro %} 