with source_users as (
    select *
    from {{ source('raw', 'users') }}
)

select
    id,
    name,
    username,
    email,
    city,
    phone,
    website,
    company
from source_users
