{{ config(materialized='table') }}

select
    date(event_ts) as event_date,
    count(distinct user_id) as daily_active_learners
from {{ ref('fact_events') }}
group by 1
order by 1 desc