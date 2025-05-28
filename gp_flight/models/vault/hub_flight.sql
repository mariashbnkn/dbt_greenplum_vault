
{{
    config(
        schema='vault',
        materialized='incremental',
        distributed_by='flight_pk',
        compresstype='zlib'
    )
}}


{%- set source_model = "stage_flight_data" -%}
{%- set src_pk = "flight_pk" -%}
{%- set src_nk = "legId" -%}
{%- set src_ldts = "load_date" -%}
{%- set src_source = "record_source" -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}