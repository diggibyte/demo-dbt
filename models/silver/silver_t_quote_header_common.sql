{{
config(
file_format='delta',
partition_by=['ing_day'],
materialized='incremental',
incremental_strategy='merge',
unique_key=['QuoteId','QuoteNumber'],
location_root='abfss://cntdlt@stexapure.dfs.core.windows.net/'~var('DBT_WSENV')~'source/silver/')
}}


{% set col_to_select = [ "ing_day",
 "QuoteId",
 "QuoteNumber",
 "DateCreated" ,
 "DateModified" ,
 "EffectiveDate" ,
 "ExternalId" ,
 "ExternalSystemId" ,
 "MarketCode" ,
 "CurrencyCode" ,
 "MarketId" ,
 "Origin" ,
 "OwnerId" ,
 "PriceBookId" ,
 "DistributionChannel" ,
 "StatusId" ,
 "StatusName" ,
 "RevisionNumber" ,
 "OpportunityId" ,
 "OpportunityName" ,
 "IsPrimary" ,
 "ErrorMessage" ,
 "QuoteTables"] %}


WITH tgt AS (
  SELECT {{ select_columns(col_to_select) }}
  from {{ source('bronze_t_quote_header', 't_quote_header')}}
  where ing_day={{  var('DBT_ING_DATE') }}
  and not(QuoteId is null or
          QuoteNumber is null or
          IsPrimary is null or
          DateModified is null)
   and CurrencyCode = 'INR'
)
SELECT *,
TO_DATE(CAST(UNIX_TIMESTAMP( cast({{ var('DBT_ING_DATE') }} AS STRING) , 'yyyyMMdd') AS TIMESTAMP)) as LOAD_DT,
current_date() as EXECUTION_DT
FROM tgt;


