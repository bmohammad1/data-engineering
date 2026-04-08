{{ config(materialized='incremental', unique_key='enrollment_id') }}

select
    enrollment_id,
    user_id,
    course_id,
    enrolled_at::timestamp as enrolled_at,
    enrollment_source,
    {{ dbt_utils.generate_surrogate_key(['enrollment_id']) }} as enrollment_sk
from {{ source('raw', 'enrollments') }}

{% if is_incremental() %}
  where enrolled_at > (select max(enrolled_at) from {{ this }})
{% endif %}