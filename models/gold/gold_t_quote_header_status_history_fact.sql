{{
config(
file_format='delta',
materialized='incremental',
incremental_strategy='merge',
location_root='abfss://cntdlt@stexapure.dfs.core.windows.net/source/gold/')
}}


SELECT
QuoteNumber as QUOTE_NO,
QuoteId as QUOTE_ID,
StatusId as STATUS_ID,
StatusName  as STATUS_NM,
OpportunityId as OPPORTUNITY_ID,
OpportunityName as OPPORTUNITY_NM,
DateCreated  as CREATED_DT,
DateModified as LAST_UPDATED_DT,
EffectiveDate as EFFECTIVE_DT,
current_date() as LOAD_DT,
current_date() as EXECUTION_DT,
DateModified as EFFECTIVE_START_DT,
"9999-12-31" as EFFECTIVE_END_DT,
true as STATUS_ACTIVE_IND
FROM  {{ ref('silver_t_quote_header_common')}}
where ing_day={{ var('ing_date') }}





