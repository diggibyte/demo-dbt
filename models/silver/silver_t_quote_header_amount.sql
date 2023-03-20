{{
config(
file_format='delta',
partition_by=['ing_day'],
materialized='incremental',
incremental_strategy='merge',
unique_key=['QuoteNumber'],
location_root='abfss://cntdlt@stexapure.dfs.core.windows.net/'~var('DBT_WSENV')~'/source/silver/')
}}

WITH tgt AS (
SELECT AverageGrossMarginPercent.Currency AS AverageGrossMarginPercent_Currency,
       AverageGrossMarginPercent.Value AS AverageGrossMarginPercent_Value,
       AverageProductDiscountPercent.Currency AS AverageProductDiscountPercent_Currency,
       AverageProductDiscountPercent.Value AS AverageProductDiscountPercent_Value,
       Cost.Currency AS Cost_Currency,
       Cost.Value AS Cost_Value,
       TotalAmount.Currency AS TotalAmount_Currency,
       TotalAmount.Value AS TotalAmount_Value,
       TotalListPrice.Currency AS TotalListPrice_Currency,
       TotalListPrice.Value AS TotalListPrice_Value,
       TotalProductDiscountAmount.Currency AS TotalProductDiscountAmount_Currency,
       TotalProductDiscountAmount.Value AS TotalProductDiscountAmount_Value,
       ing_day,
       QuoteNumber
FROM  {{ source('bronze_t_quote_header', 't_quote_header')}}

where ing_day={{ var('ing_date') }}
)
select * ,
TO_DATE(CAST(UNIX_TIMESTAMP( cast({{ var('ing_date') }} AS STRING) , 'yyyyMMdd') AS TIMESTAMP)) as LOAD_DT,
current_date() as EXECUTION_DT
from tgt where
         AverageGrossMarginPercent_Currency ='INR'
        and AverageProductDiscountPercent_Currency ='INR'
        and  TotalListPrice_Currency ='INR'
        and TotalProductDiscountAmount_Currency ='INR'







