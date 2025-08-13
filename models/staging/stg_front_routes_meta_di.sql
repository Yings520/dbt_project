{{ config(
    materialized='incremental',
    unique_key='path'
) }}

with latest_partition as (
    select max(pt) as max_pt
    from {{ source('PUBLIC', 'raw_external_front_routes_meta_di') }}
)
select
path ,
method ,
description ,
summary ,
tags ,
security ,
parameters ,
responses ,
navigation_flow ,
page_renders ,
user_journey ,
etl_time ,
pt 
from {{ source('PUBLIC', 'raw_external_front_routes_meta_di') }} t
join latest_partition lp
  on t.pt = lp.max_pt

{% if is_incremental() %}
where t.pt > (select max(pt) from {{ this }})
{% endif %}
