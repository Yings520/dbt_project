{{ config(
    materialized='incremental',
    unique_key= ['pt', 'path']
) }}


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


{% if is_incremental() %}
where t.pt > (select max(pt) from {{ this }})
{% endif %}
