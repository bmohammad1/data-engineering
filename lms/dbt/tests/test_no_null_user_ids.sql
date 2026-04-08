-- Fail if staging has NULL user_id
select *
from {{ ref('stg_users') }}
where user_id is null;