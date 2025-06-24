   {% macro add_columns_to_table(target_table, target_schema, project=target.project, dataset=target.dataset, dry_run=True) %}
    
    {% set get_add_column_commands_query %}
    SELECT 
        'ALTER TABLE `{{project}}.'|| table_schema ||'.'|| '{{target_table}}' ||'` ADD COLUMN '|| column_name || ' '|| REPLACE(REPLACE(REPLACE(data_type, 'BOOL', 'STRING'), 'FLOAT64', 'STRING'),'INT64','STRING')  ||';' AS sql_cmd
    FROM `{{project}}.region-us.INFORMATION_SCHEMA.COLUMNS` t
    where t.table_schema = '{{target_schema}}'
            AND t.table_name = '{{target_table}}' || '_new_differential'
            AND LOWER(t.column_name) NOT IN (
            select LOWER(t.column_name) 
            from `{{project}}.region-us.INFORMATION_SCHEMA.COLUMNS` t
            where t.table_schema = '{{target_schema}}'
                AND t.table_name = '{{target_table}}')
    ORDER BY t.ordinal_position
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