   {% macro execute_next_merging_macro(next_macro, target_table, target_schema, project=target.project, dry_run=True) %}

    {{ log('Excuting macro ' ~ next_macro, info=True) }}
    {{ log('Check dry_run parameter: ' ~ dry_run, info=True) }}
    {% if dry_run %}
        {{ log('Since the Dry Run parameter is set to ' ~ dry_run ~ ' then do not proceed to next macro.', info=True) }}
    {% else %}
        {{ log('Since the Dry Run parameter is set to ' ~ dry_run ~ ' then proceed to ' ~ next_macro ~ '.', info=True) }}
        {{ log('Running the next merge macro: ' ~ next_macro, info=True) }}
        {% if next_macro == 'copy_non_string_schema_to_new_differential' %}
            {{ copy_non_string_schema_to_new_differential(target_table, target_schema, project, dry_run) }}
            {{ return('') }}
        {% endif %}
        {% if next_macro == 'add_columns_to_table' %}
            {{ add_columns_to_table(target_table, target_schema, project, dry_run) }}
            {{ return('') }}
        {% endif %}
        {% if next_macro == 'inserttable_from_new_differential' %}
            {{ inserttable_from_new_differential(target_table, target_schema, project, dry_run) }}
            {{ return('') }}
        {% endif %}
        {% if next_macro == 'updatetable_from_new_differential' %}
            {{ updatetable_from_new_differential(target_table, target_schema, project, arg_dry_run) }}
            {{ return('') }}
        {% endif %}
        {{ log('There was a macro executed that missed each type.', info=True) }}
    {% endif %}

{% endmacro %} 