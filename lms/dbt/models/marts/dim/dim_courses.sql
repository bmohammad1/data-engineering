{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['course_id']) }} as course_sk,
    course_id,
    title,
    category,
    difficulty,
    created_at
from {{ ref('stg_courses') }}