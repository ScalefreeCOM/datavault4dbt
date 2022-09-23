{{ config(materialized='view', 
          schema='scalefree_public_edw') }}

{%- set yaml_metadata -%}
source_model:
    'stackoverflow': 'comments'
hashed_columns: 
    hk_comments_h:
        - id
    hk_comments_l:
        - id
        - post_id
        - user_id
    hk_posts_h:
        - post_id
    hk_users_h:
        - user_id
    hd_comments_n_s:
        is_hashdiff: true
        columns:
            - text
            - creation_date
            - user_display_name
            - score        
rsrc: "!bigquery-public-data.stackoverflow.comments"
ldts: "TIMESTAMP_TRUNC(TIMESTAMP_ADD(creation_date, INTERVAL 1 DAY), DAY)"
include_source_columns: true
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{{ datavault4dbt.stage(include_source_columns=metadata_dict['include_source_columns'],
                  source_model=metadata_dict['source_model'],
                  hashed_columns=metadata_dict['hashed_columns'],
                  rsrc=metadata_dict['rsrc'],
                  ldts=metadata_dict['ldts']) }}