
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
{%- set src_hashdiff = "flight_hashdiff" -%}
{%- set src_payload = ["travelduration, 
                        elapseddays,
                        isbasiceconomy, 
                        isrefundable, 
                        isnonstop, 
                        basefare, 
                        totalfare, 
                        seatsremaining, 
                        totaltraveldistance, 
                        segmentsdeparturetimeepochseconds, 
                        segmentsdeparturetimeraw, 
                        segmentsarrivaltimeepochseconds, 
                        segmentsarrivaltimeraw, 
                        segmentsarrivalairportcode, 
                        segmentsdepartureairportcode, 
                        segmentsairlinename, 
                        segmentsairlinecode, 
                        segmentsequipmentdescription, 
                        segmentsdurationinseconds, 
                        segmentsdistance, 
                        segmentscabincode, 
                        farebasiscode"] -%}
{%- set src_nk = "legId" -%}
{%- set src_eff = "effective_from" -%}
{%- set src_ldts = "load_date" -%}
{%- set src_source = "record_source" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}