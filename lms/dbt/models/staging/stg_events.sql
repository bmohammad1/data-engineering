{{ config(materialized='incremental', unique_key='event_id') }}
select
    payload:id::varchar                  as event_id,
    payload:actor.account.name::varchar  as user_id,
    payload:object.id::varchar           as course_id,
    payload:verb.id::varchar             as event_type,
    payload:timestamp::timestamp         as event_ts,
    payload:result.duration::varchar     as raw_duration,
    payload:result.rating::float         as rating,
    payload:context.platform::varchar    as device_type,
    payload:context.app_version::varchar as app_version,
    payload:context.device::varchar      as user_agent,
    payload:context.ip::varchar          as ip_address,
    payload:context.session_id::varchar  as session_id,
    {{ dbt_utils.generate_surrogate_key(['payload:id']) }} as event_sk
from {{ source('raw', 'events') }}
-- select
--     event_id,
--     user_id,
--     course_id,
--     event_type,
--     event_ts::timestamp as event_ts,
--     time_spent_sec,
--     rating,
--     device_type,
--     app_version,
--     user_agent,
--     ip_address,
--     session_id,
--     {{ dbt_utils.generate_surrogate_key(['event_id']) }} as event_sk
-- from {{ source('raw', 'events') }}

{% if is_incremental() %}
  where event_ts > (select max(event_ts) from {{ this }})
{% endif %}