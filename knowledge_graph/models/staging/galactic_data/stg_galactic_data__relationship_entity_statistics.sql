with
    source as (
        select * from {{ source('galactic_data', 'relationship_entity_statistics') }}
    )

select entity_id, entity_name,
from source
