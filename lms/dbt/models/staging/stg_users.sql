{{ config(materialized='view') }}

select
    user_id,
    initcap(name) as user_name,
    upper(country) as country_code,
    created_at::timestamp as created_at,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk
from {{ source('raw', 'users') }}