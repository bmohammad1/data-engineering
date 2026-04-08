-- TODO snapshot later
{% snapshot users_snapshot %}
{{ config(
    target_schema='history',
    unique_key='user_id',
    strategy='timestamp',
    updated_at='created_at'
) }}

select
  * 
from {{ source('raw', 'users') }}

{% endsnapshot %}