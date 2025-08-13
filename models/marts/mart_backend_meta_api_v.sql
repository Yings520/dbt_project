{{ config(
    materialized='view'
) }}

select
    path,
    method,
    description,
    tags,
    security,
    parameters,
    request_body,
    responses,
    etl_time,
    pt
from {{ ref('stg_backend_meta_api_di') }}
qualify pt = max(pt) over ()