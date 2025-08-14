{{ config(
    materialized='incremental',
    unique_key = ['pt','operation_id']
) }}


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
from {{ source('PUBLIC', 'raw_external_backend_api_meta_di') }} t

{% if is_incremental() %}
where t.pt > (select max(pt) from {{ this }})
{% endif %}
