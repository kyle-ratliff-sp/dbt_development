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


    {% if dry_run %}
        {{ log('Since the Dry Run parameter is set to ' ~ dry_run ~ ' then do not proceed to next macro.', info=True) }}
    {% else %}
        {{ log('Since the Dry Run parameter is set to ' ~ dry_run ~ ' then proceed to add_columns_to_table.', info=True) }}
        {#- Define the arguments to pass to the add_columns_to_table macro -#}
        {% set arg_target_table = target_table %}
        {% set arg_target_schema = target_schema %}
        {% set arg_project = project %}
        {% set arg_dry_run = dry_run %}
        {{ log('Printing arg_target_table: ' ~ arg_target_table, info=True) }}
        {{ log('Printing arg_target_schema: ' ~ arg_target_schema, info=True) }}
        {{ log('Printing arg_targearg_projectt_table: ' ~ arg_project, info=True) }}
        {{ log('Printing arg_dry_run: ' ~ arg_dry_run, info=True) }}
    {% endif %}

  {#- Call the child macro with arguments -#}
  {#-{{ child_macro_with_args(arg1_value, arg2_value) }}-#}
    
{% endmacro %} 