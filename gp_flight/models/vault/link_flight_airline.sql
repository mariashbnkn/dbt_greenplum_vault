
{{
    config(
        schema='vault',
        materialized='incremental',
        compresslevel=4,
        distributed_by='link_flight_airline_pk',
        compresstype='zlib'
    )
}}


{%- set source_model = "stage_flight_data" -%}
{%- set src_pk = "link_flight_airline_pk" -%}
{%- set src_fk = ["flight_pk", "airline_pk"] -%}
{%- set src_ldts = "load_date" -%}
{%- set src_source = "record_source" -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}