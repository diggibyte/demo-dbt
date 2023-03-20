{% macro remove_spcl_char(columns_name) %}
    {{ columns_name.upper().replace(" ", "_").replace("__", "_").replace("-", "_").replace("_party", "").replace("District_Manager", "DM").replace("Regional_Manager", "RM").replace("ADDRESS_NM", "STREET_NM") }}
{%- endmacro %}


{% macro involved_party_rename(columns_list) %}
{% set select_clause = [] %}
{% for c in columns_list %}
  {% do select_clause.append("MAX(CASE WHEN InvolvedParties_PartnerFunctionName = '" ~ c ~ "' THEN InvolvedParties_POBoxPostalCode ELSE null END) AS " ~ remove_spcl_char(c) ~"_POSTAL_CD" ) %}
  {% do select_clause.append("MAX(CASE WHEN InvolvedParties_PartnerFunctionName = '" ~ c ~ "' THEN InvolvedParties_AddressName ELSE null END) AS " ~ remove_spcl_char(c)~"_ADDRESS_NM" ) %}
  {% do select_clause.append("MAX(CASE WHEN InvolvedParties_PartnerFunctionName = '" ~ c ~ "' THEN InvolvedParties_CityName ELSE null END) AS " ~ remove_spcl_char(c)~"_CITY_NM" ) %}
  {% do select_clause.append("MAX(CASE WHEN InvolvedParties_PartnerFunctionName = '" ~ c ~ "' THEN InvolvedParties_Country ELSE null END) AS " ~ remove_spcl_char(c)~"_COUNTRY" ) %}
  {%- endfor %}
{% set column_str = select_clause | join(', ') %}
    {{ column_str }}
{%- endmacro %}