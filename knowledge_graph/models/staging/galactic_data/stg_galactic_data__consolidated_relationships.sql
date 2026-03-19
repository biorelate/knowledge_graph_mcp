with
    source as (
        select * from {{ source('galactic_data', 'consolidated_relationships') }}
    )

select
    entity1_id,
    entity2_id,
    total_mentions,
    associates_with_count,
    binding_count,
    biomarker_count,
    causes_no_change_count,
    decreases_count,
    increases_count,
    regulates_count,
    treats_count,
from source
