{{ config(
    materialized='incremental',
    unique_key='operation_id'
) }}

with latest_partition as (
    select max(pt) as max_pt
    from {{ source('PUBLIC', 'raw_external_backend_meta_api_di') }}
)
select
    PATH,
    METHOD,
    DESCRIPTION,
    OPERATION_ID,
    TAGS,
    SECURITY,
    PARAMETERS,
    REQUEST_BODY,
    RESPONSES,
    ETL_TIME,
    PT
from {{ source('PUBLIC', 'raw_external_backend_meta_api_di') }} t
join latest_partition lp
  on t.pt = lp.max_pt

{% if is_incremental() %}
where t.pt > (select max(pt) from {{ this }})
{% endif %}
