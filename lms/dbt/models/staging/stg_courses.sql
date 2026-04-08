{{ config(materialized='view') }}

select
    course_id,
    title,
    category,
    difficulty,
    created_at::timestamp as created_at,
    {{ dbt_utils.generate_surrogate_key(['course_id']) }} as course_sk
from {{ source('raw', 'courses') }}