   {% macro inserttable_from_new_differential(target_table, target_schema, primary_key, project=target.project, dry_run=True) %}
    
    {% set get_add_column_commands_query %}
    SELECT 
        'INSERT INTO `{{project}}.'|| '{{target_schema}}' ||'.'|| '{{target_table}}' ||'` ('|| STRING_AGG(column_name) ||') SELECT * FROM `{{project}}.'|| '{{target_schema}}' ||'.'|| '{{target_table}}' ||'_new_differential` AS diff WHERE NOT EXISTS (SELECT 1 FROM `{{project}}.'|| '{{target_schema}}' ||'.'|| '{{target_table}}' ||'` AS ori WHERE ori.'|| '{{primary_key}}' ||' = diff.'|| '{{primary_key}}' ||');' AS sql_cmd
    FROM `{{project}}.region-us.INFORMATION_SCHEMA.COLUMNS` t
    where t.table_schema = '{{target_schema}}'
        AND t.table_name = '{{target_table}}' || '_new_differential'
        AND LOWER(t.column_name) IN (
            select LOWER(t.column_name) 
        from `sandbox-444621.region-us.INFORMATION_SCHEMA.COLUMNS` t
        where t.table_schema = '{{target_schema}}'
            AND t.table_name = '{{target_table}}')
    {% endset %}

    {{ log('Print Insert Table query:' + get_add_column_commands_query, info=True)}}

    {{ log('\nGenerating Insert queries...\n', info=True) }}
    {% set create_queries = run_query(get_add_column_commands_query).columns[0].values() %}

    {% for query in create_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Inserting object with command: ' ~ query, info=True) }}
            {% do run_query(query) %} 
        {% endif %}       
    {% endfor %}
    
{% endmacro %} 