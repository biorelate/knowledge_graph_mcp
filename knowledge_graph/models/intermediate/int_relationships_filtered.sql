/*
Unpivot the consolidated_relationships staging model into a long-format table,
filtering by a minimum evidence threshold and restricting both entities to the
configured ontologies. Produces one row per (entity1, relationship_type, entity2)
triple.
*/
with
    associates_with_relationships as (
        select
            entity1_id,
            "associates_with" as relationship_type,
            entity2_id,
            associates_with_count / total_mentions as proportion,
            associates_with_count as evidence_count,
        from {{ ref('stg_galactic_data__consolidated_relationships') }}
        where associates_with_count >= {{ var('min_evidence') }}
    ),
    binding_relationships as (
        select
            entity1_id,
            "binding" as relationship_type,
            entity2_id,
            binding_count / total_mentions as proportion,
            binding_count as evidence_count,
        from {{ ref('stg_galactic_data__consolidated_relationships') }}
        where binding_count >= {{ var('min_evidence') }}
    ),
    biomarker_relationships as (
        select
            entity1_id,
            "biomarker" as relationship_type,
            entity2_id,
            biomarker_count / total_mentions as proportion,
            biomarker_count as evidence_count,
        from {{ ref('stg_galactic_data__consolidated_relationships') }}
        where biomarker_count >= {{ var('min_evidence') }}
    ),
    causes_no_change_relationships as (
        select
            entity1_id,
            "causes_no_change" as relationship_type,
            entity2_id,
            causes_no_change_count / total_mentions as proportion,
            causes_no_change_count as evidence_count,
        from {{ ref('stg_galactic_data__consolidated_relationships') }}
        where causes_no_change_count >= {{ var('min_evidence') }}
    ),
    decreases_relationships as (
        select
            entity1_id,
            "decreases" as relationship_type,
            entity2_id,
            decreases_count / total_mentions as proportion,
            decreases_count as evidence_count,
        from {{ ref('stg_galactic_data__consolidated_relationships') }}
        where decreases_count >= {{ var('min_evidence') }}
    ),
    increases_relationships as (
        select
            entity1_id,
            "increases" as relationship_type,
            entity2_id,
            increases_count / total_mentions as proportion,
            increases_count as evidence_count,
        from {{ ref('stg_galactic_data__consolidated_relationships') }}
        where increases_count >= {{ var('min_evidence') }}
    ),
    regulates_relationships as (
        select
            entity1_id,
            "regulates" as relationship_type,
            entity2_id,
            regulates_count / total_mentions as proportion,
            regulates_count as evidence_count,
        from {{ ref('stg_galactic_data__consolidated_relationships') }}
        where regulates_count >= {{ var('min_evidence') }}
    ),
    treats_relationships as (
        select
            entity1_id,
            "treats" as relationship_type,
            entity2_id,
            treats_count / total_mentions as proportion,
            treats_count as evidence_count,
        from {{ ref('stg_galactic_data__consolidated_relationships') }}
        where treats_count >= {{ var('min_evidence') }}
    ),
    relationships as (
        select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
        from associates_with_relationships
        union all
        select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
        from binding_relationships
        union all
        select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
        from biomarker_relationships
        union all
        select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
        from causes_no_change_relationships
        union all
        select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
        from decreases_relationships
        union all
        select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
        from increases_relationships
        union all
        select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
        from regulates_relationships
        union all
        select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
        from treats_relationships
    )

select entity1_id, relationship_type, entity2_id, proportion, evidence_count,
from relationships
where
    -- Filter to only include relationships where both entities belong to the
    -- specified ontologies
    lower(split(entity1_id, ":")[0])
    in ({{ "'" ~ var('ontologies')|join("','") ~ "'" }})
    and lower(split(entity2_id, ":")[0])
    in ({{ "'" ~ var('ontologies')|join("','") ~ "'" }})
