{{ config(materialized='view', 
          schema='scalefree_public_edw') }}

{%- set yaml_metadata -%}
source_model:
    'stackoverflow': 'users'
hashed_columns: 
    hk_users_h:
        - id
    hd_users_n_s:
        is_hashdiff: true
        columns:
            - creation_date
            - last_access_date
            - reputation
            - up_votes
            - down_votes
            - views
            - website_url
    hd_users_p_s:
        is_hashdiff: true
        columns:
            - display_name
            - about_me
            - age
            - location
            - profile_image_url      
rsrc: "!bigquery-public-data.stackoverflow.users"
ldts: "TIMESTAMP_TRUNC(TIMESTAMP_ADD(creation_date, INTERVAL 1 DAY), DAY)"
include_source_columns: true
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_metadata) -%}

{{ dbtvault_scalefree.stage(include_source_columns=metadata_dict['include_source_columns'],
                  source_model=metadata_dict['source_model'],
                  hashed_columns=metadata_dict['hashed_columns'],
                  rsrc=metadata_dict['rsrc'],
                  ldts=metadata_dict['ldts']) }}