{%- macro clean_up_pit(snapshot_relation) -%}

DELETE {{ this }} pit
WHERE pit.sdts not in (SELECT sdts FROM {{ ref(snapshot_relation) }} snap WHERE is_active=TRUE)

{%- endmacro -%}
