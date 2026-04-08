{{ config(
    materialized='incremental',
    unique_key='enrollment_id',
    incremental_strategy='merge'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['enrollment_id']) }} as enrollment_sk,
    enrollment_id,
    user_id,
    course_id,
    enrolled_at,
    enrollment_source,

    case 
        when enrollment_source ilike '%organic%' then 'organic'
        when enrollment_source ilike '%paid%' then 'paid'
        else 'unknown'
    end as enrollment_channel

from {{ ref('stg_enrollments') }}

{% if is_incremental() %}
  where enrolled_at > (select coalesce(max(enrolled_at), '1900-01-01') from {{ this }})
{% endif %}