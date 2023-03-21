{{
config(
file_format='delta',
partition_by=['ing_day'],
materialized='incremental',
incremental_strategy='merge',
unique_key=['QuoteId','QuoteNumber'],
location_root='abfss://cntdlt@stexapure.dfs.core.windows.net/'~var('DBT_WSENV')~'source/silver/')
}}

{% set col_to_select_and_rename = [ "AdditionalStreetPrefixName", "AdditionalStreetSuffixName", "AddressAdditionalName", "AddressName", "AddressName3", "AddressName4", "BankAccount1", "BankAccount2", "BusinessPartnerId", "CityName", "CorrespondenceLanguage", "Country", "District", "EmailAddress", "ExternalId", "Fax", "FirstName", "FormOfAddress", "HouseNumber", "Id", "LastName", "MobilePhone", "Name", "POBox", "POBoxPostalCode", "PartnerFunctionId", "PartnerFunctionKey", "PartnerFunctionName", "PartnerId", "PartnerNumber", "Phone", "PostalCode", "PrimaryIndustry", "Region", "State", "StreetName", "StreetPrefixName", "StreetSuffixName", "SystemId", "TaxJurisdiction", "TaxNumber1", "TaxNumber2", "TimeZone", "TransportZone", "VATNumber"] %}
{% set rename_val = "InvolvedParties_" %}

WITH temp AS (
  select QuoteId, QuoteNumber, ing_day, ips.*
  from {{ source('bronze_t_quote_header', 't_quote_header')}}
  LATERAL VIEW explode(InvolvedParties) ip as ips
  where ing_day={{ var('DBT_ING_DATE')  }}
)
SELECT QuoteId, QuoteNumber,ing_day,
 {{ select_columns_and_append_reename(col_to_select_and_rename,rename_val) }},
TO_DATE(CAST(UNIX_TIMESTAMP( cast({{ var('DBT_ING_DATE')  }} AS STRING) , 'yyyyMMdd') AS TIMESTAMP)) as LOAD_DT,
current_date() as EXECUTION_DT
FROM temp;





