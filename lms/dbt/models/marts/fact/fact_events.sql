{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as event_sk,
    event_id,
    user_id,
    course_id,
    event_type,
    event_ts,
    time_spent_sec,
    rating,
    device_type,
    app_version,
    ip_address,
    session_id,

    -- COMPUTED METRICS
    case when event_type = 'complete' then 1 else 0 end as is_completed,

    case when rating > 0 then 1 else 0 end as has_rating,
    nullif(rating,0) as safe_rating,

    greatest(coalesce(time_spent_sec,0), 0) as safe_time_spent_sec,

    case 
        when event_type in ('view','start','complete','rate')
             then event_type
        else 'other'
    end as event_category,

    (
        case when event_type = 'complete' then 5 else 0 end +
        case when event_type = 'start' then 3 else 0 end +
        case when event_type = 'view' then 1 else 0 end +
        least( coalesce(time_spent_sec,0) / 60 , 5 ) +
        case when rating > 0 then 2 else 0 end
    ) as engagement_score

from {{ ref('stg_events') }}

{% if is_incremental() %}
  where event_ts > (select coalesce(max(event_ts), '1900-01-01') from {{ this }})
{% endif %}