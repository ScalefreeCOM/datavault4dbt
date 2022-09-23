{{ config(materialized='view', 
          schema='scalefree_public_edw') }}

{%- set yaml_metadata -%}
source_model:
    'stackoverflow': 'stackoverflow_posts'
hashed_columns: 
    hk_posts_h:
        - id
    hk_posts_l:
        - id
        - accepted_answer_id
        - owner_user_id
        - parent_id
    hk_parent_posts_h:
        - parent_id
    hk_users_h:
        - owner_user_id
    hk_comments_h:
        - accepted_answer_id
    hd_posts_n_s:
        is_hashdiff: true
        columns:
            - title
            - body
            - answer_count
            - comment_count
            - community_owned_date
            - creation_date
            - favorite_count
            - last_activity_date
            - last_edit_date
            - last_editor_display_name
            - last_editor_user_id
            - owner_display_name
            - post_type_id
            - score
            - tags
            - view_count       
rsrc: "!bigquery-public-data.stackoverflow.stackoverflow_posts"
ldts: "TIMESTAMP_TRUNC(TIMESTAMP_ADD(creation_date, INTERVAL 1 DAY), DAY)"
include_source_columns: true
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{{ datavault4dbt.stage(include_source_columns=metadata_dict['include_source_columns'],
                  source_model=metadata_dict['source_model'],
                  hashed_columns=metadata_dict['hashed_columns'],
                  rsrc=metadata_dict['rsrc'],
                  ldts=metadata_dict['ldts']) }}