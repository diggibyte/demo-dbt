{{
config(
file_format='delta',
partition_by=['ing_day'],
materialized='incremental',
incremental_strategy='merge',
unique_key=['QuoteId','Quote_Number'],
location_root="abfss://cntdlt@stexapure.dfs.core.windows.net/'{{ env_var('DBT_WSENV') }}'/source/silver/")
}}

{% set col_to_pivot = [  "Opportunity Name",
"Quote Expiration Date",
"Customer Comment",
"MRCInUse",
"Customer's Language",
"Quote Number",
"ZCPQ_QT_PROJECTLOCATION",
"ZCPQ_QT_PROJ_LOC_ADD",
"ZCPQ_QT_CLOSE_DATE",
"ZCPQ_QT_LIVING_UNITS",
"ZCPQ_QT_MIX_COMMITMENT",
"ZCPQ_QT_UPGRADE_COMMITMENT",
"ZCPQ_QT_FIRST_ESTIMATEDSHIPTO",
"ZCPQ_QT_LAST_ESTIMATEDSHIPTO",
"ZCPQ_QT_SALES_STAGE",
"ZCPQ_QT_FINISH_BLEND_MARGIN",
"ZCPQ_QT_SERV_BLEND_MARGIN",
"ZCPQ_QT_OVERALL_BLEND_MARGIN",
"ZCPQ_QT_ORIGIN",
"ZCPQ_QT_REF_OPPERTUNITY",
"ZCPQ_QT_Service_Level",
"Account_ID",
"RequestedDate",
"ZCPQ_QT_QUOTE_TYPE",
"ZCPQ_QT_PRIORITY",
"ZCPQ_QT_REASON",
"ZCPQ_QT_TAXEXEMPT",
"ZCPQ_QT_SHP_FREQUENCY",
"ZCPQ_QT_OPPERTUNITYGROUP",
"ZCS_CPQ_QU_hidden_cust_fld",
"SCRIPT_FLAG",
"ZCPQ_QT_INVENTORY_TYPE",
"ZCS_CF_ExcelUpload",
"ZCPQ_QT_OWNERSHIP_BLEND_MARGIN",
"ZCPQ_QT_SECONDARY_STATUS",
"ZCPQ_QT_SENT_TO_CUSTOMER",
"Approval_to",
"ZCS_CF_MULTIYEAR",
"ZCS_CF_UPLIFT",
"ZCS_CF_SELECTEDITMS",
"ZCPQ_QT_ReferenceQuote",
"ZCPQ_QT_Description",
"ZCPQ_QT_COPY_LINE_ITEMS",
"ZCPQ_QT_ChangeQuote",
"ZCPQ_QT_ACCEPTED_DATE",
"Mul_Yr_Default_Uplift",
"ZCPQ_QT_NUMBER_OF_DAYS",
"ZCPQ_QT_30_Days_Ind",
"Project Name",
"Additional Comments",
"Terms",
"PO Number",
"Customer Quote Reference",
"Invoice Number",
"Tracking Number"]  %}
{% set rename_val = "InvolvedParties_" %}


with upper as (

WITH temp AS (
 select
 QuoteId,
 Custm_fields.*,
 ing_day
  from {{ source('bronze_t_quote_header', 't_quote_header')}}
 LATERAL VIEW explode(CustomFields) cf as Custm_fields
  where ing_day={{ env_var('DBT_ING_DATE') }}
)
 SELECT QuoteId,ing_day,
  {{ pivot_cols_with_rename(col_to_pivot) }}
FROM temp
group by
QuoteId,ing_day
)
select *,
TO_DATE(CAST(UNIX_TIMESTAMP( cast({{ env_var('DBT_ING_DATE') }} AS STRING) , 'yyyyMMdd') AS TIMESTAMP)) as LOAD_DT,
current_date() as EXECUTION_DT
from upper







