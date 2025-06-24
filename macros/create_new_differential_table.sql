 {% macro create_new_differential_table(target_table, project=target.project, dataset=target.dataset, dry_run=True) %}
    
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
    AND upper(table_schema) = upper('{{ dataset }}')
    {% endset %}

    {{ log('Print Create DDL command:' + get_create_commands_query, info=True)}}

    {{ log('\nGenerating create queries...\n', info=True) }}
    {% set create_queries = run_query(get_create_commands_query).columns[0].values() %}

    {% for query in create_queries %}
        {% if dry_run %}
            {{ log(query, info=True) }}
        {% else %}
            {{ log('Dropping object with command: ' ~ query, info=True) }}
            {% do run_query(query) %} 
        {% endif %}       
    {% endfor %}
    
{% endmacro %} 