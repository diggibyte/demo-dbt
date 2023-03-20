{% macro select_columns(columns_list) %}
{% set column_str = columns_list | join(', ') %}
    {{ column_str }}
{% endmacro %}


{% macro select_columns_and_append_reename(columns_list,append_val) %}
{% set rename_map = {} %}
{% for item in columns_list %}
  {% set rename_map = rename_map.update({ item: append_val ~ item}) %}
{% endfor %}
{% set select_clause = [] %}
{% for k, v in rename_map.items() %}
  {% do select_clause.append(k ~ ' AS ' ~ v) %}
{% endfor %}
{% set column_str = select_clause | join(', ') %}
    {{ column_str }}
{% endmacro %}


{% macro pivot_cols_with_rename(columns_list) %}
{% set select_clause = [] %}
{% for c in columns_list %}
  {% do select_clause.append("MAX(CASE WHEN Name = '" ~ c.replace("\'", "\\'") ~ "' THEN Content END) AS " ~ c.replace("\'", "").replace(" ", "_")) %}
{% endfor %}
{% set column_str = select_clause | join(', ') %}
    {{ column_str }}
{% endmacro %}