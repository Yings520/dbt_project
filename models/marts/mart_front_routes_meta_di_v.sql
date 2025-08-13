{{ config(
    materialized='view'
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
from {{ ref('stg_front_routes_meta_di') }}
qualify pt = max(pt) over ()