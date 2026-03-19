/*  macro for selecting relationships by relationship type
*/
{% macro select_relationship_by_type(_relationship_type) %}
    select
        entity1_id,
        "{{ _relationship_type }}" as relationship_type,
        entity2_id,
        proportion,
        evidence_count,
    from {{ ref('int_relationships_filtered') }} as r
    where r.relationship_type = "{{ _relationship_type }}"
{% endmacro %}
