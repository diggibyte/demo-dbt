{{
config(
file_format='delta',
materialized='incremental',
incremental_strategy='merge',
location_root="abfss://cntdlt@stexapure.dfs.core.windows.net/'{{ env_var('DBT_WSENV') }}'/source/gold/")
}}

{% set col_to_select = [ "Ship-to party",
 "Payer",
 "Regional Manager",
 "District  Manager" ,
 "Project Location" ,
 "Bill-to party" ] %}




with qoute_header  as (
select
{{ dbt_utils.star(from=ref('silver_t_quote_header_common'), except=["QuoteTables"]) }}
FROM  {{ ref('silver_t_quote_header_common')}}
where ing_day={{ env_var('DBT_ING_DATE') }}

)
, invol_party  as (
select
QuoteId,
QuoteNumber,
 {{ involved_party_rename(col_to_select) }}
FROM  {{ ref('silver_t_quote_header_involved_parties')}}
where ing_day={{ env_var('DBT_ING_DATE') }}
group by
QuoteId, QuoteNumber
)
select
a.DateCreated,
a.DateModified,
a.EffectiveDate,
a.ExternalId,
a.ExternalSystemId,
a.MarketCode,
a.CurrencyCode,
a.MarketId,
a.Origin,
a.OwnerId,
a.PriceBookId,
a.DistributionChannel,
a.StatusId,
a.StatusName,
a.RevisionNumber,
a.OpportunityId,
a.OpportunityName,
a.IsPrimary,
a.ErrorMessage,
a.LOAD_DT,
a.EXECUTION_DT , b.*
from
qoute_header a left outer join invol_party b
on a.QuoteId = b.QuoteId
and a.QuoteNumber = b.QuoteNumber





