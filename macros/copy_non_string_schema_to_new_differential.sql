   {% macro copy_non_string_schema_to_new_differential(target_table, target_schema, project=target.project, dry_run=True) %}
    
    {% set get_add_column_commands_query %}
    select 
        'INSERT `{{project}}.'|| target_schema ||'.'|| target_table ||'_new_differential` ('|| STRING_AGG(column_name) ||') SELECT '|| STRING_AGG(CASE WHEN data_type != 'STRING' AND data_type NOT LIKE 'STRUCT<%' THEN 'CAST ('||column_name||' AS STRING)'ELSE column_name END)||' FROM `{{project}}.'|| target_schema ||'.'|| target_table ||'_non_string_schema`;' AS sql_cmd
    from `{{project}}.region-us.INFORMATION_SCHEMA.COLUMNS` t
        where t.table_schema = target_schema
        AND t.table_name = target_table || '_non_string_schema'
    {% endset %}

    {{ log('Print Add Column query:' + get_add_column_commands_query, info=True)}}

    {{ log('\nGenerating alter queries...\n', info=True) }}
    {% set create_queries = run_query(get_add_column_commands_query).columns[0].values() %}

    {% for query in create_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Altering object with command: ' ~ query, info=True) }}
            {% do run_query(query) %} 
        {% endif %}       
    {% endfor %}
    
{% endmacro %} 