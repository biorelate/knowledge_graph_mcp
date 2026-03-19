/*
Retrieve all unique entities involved in relationships from the
int_relationships_filtered model, enriched with entity names.
*/
with
    ontology_ids as (
        select distinct entity1_id as entity_id
        from {{ ref('int_relationships_filtered') }}
        union distinct
        select distinct entity2_id as entity_id
        from {{ ref('int_relationships_filtered') }}
    )

select e.entity_id, e.entity_name
from ontology_ids as oi
join
    {{ ref('stg_galactic_data__relationship_entity_statistics') }} as e using (
        entity_id
    )
