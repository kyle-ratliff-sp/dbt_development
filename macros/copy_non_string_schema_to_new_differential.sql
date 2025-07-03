   {% macro copy_non_string_schema_to_new_differential(target_table, target_schema, project=target.project, dry_run=True) %}

   {%- set full_table_name -%}
   {{project ~ '.' ~ target_schema ~ '.' ~ target_table}}
   {%- endset -%}
    
    {% set get_add_column_commands_query %}
    select 
        'INSERT `{{full_table_name ~ '_new_differential'}}` ('|| STRING_AGG(column_name) ||') SELECT '|| STRING_AGG(CASE WHEN data_type != 'STRING' AND data_type NOT LIKE 'STRUCT<%' THEN 'CAST ('||column_name||' AS STRING)'ELSE column_name END)||' FROM `{{full_table_name ~ '_non_string_schema'}}`;' AS sql_cmd
    from `{{project}}.region-us.INFORMATION_SCHEMA.COLUMNS` t
        where t.table_schema = '{{target_schema}}'
        AND t.table_name = '{{target_table ~ '_non_string_schema'}}'
    {% endset %}

    {{ log('Print copy from non string schema query query:' + get_add_column_commands_query, info=True)}}

    {{ log('\nGenerating insert query...\n', info=True) }}
    {% set create_queries = run_query(get_add_column_commands_query).columns[0].values() %}

    {% for query in create_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Insert object with command: ' ~ query, info=True) }}
            {% do run_query(query) %} 
        {% endif %}
    {% endfor %}

    {% set next_macro_arg = "add_columns_to_table" %}
    {{ log('Attempting to execute next stage of the merging process: ' ~ next_macro_arg, info=True) }}
    {{ log('The dry_run parameter is as follows: '  ~ dry_run, info=True) }}
    {{ execute_next_merging_macro(next_macro=next_macro_arg, target_table=target_table, target_schema=target_schema, project=project,dry_run=dry_run) }}
    
{% endmacro %} 