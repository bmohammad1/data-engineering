-- -- placeholder
-- select 1 as dummy where false
{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
    user_id,
    user_name,
    country_code,
    created_at
from {{ ref('stg_users') }}