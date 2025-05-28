{{
    config(
        schema='stage',
        materialized='table',
        appendonly='true',
        orientation='column',
        compresstype='zlib',
        compresslevel=4,
        blocksize=32768
    )
}}


{%- set yaml_metadata -%}
source_model: 'raw_flight_data'
derived_columns:
  LOAD_DATE: (searchDate + 1 * INTERVAL '1 day')
  EFFECTIVE_FROM: 'searchDate'
hashed_columns:
  FLIGHT_PK:
    - 'legId'
  FLIGHT_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'travelduration'
      - 'elapseddays'
      - 'isbasiceconomy'
      - 'isrefundable'
      - 'isnonstop'
      - 'basefare'
      - 'totalfare'
      - 'seatsremaining'
      - 'totaltraveldistance'
      - 'segmentsdeparturetimeepochseconds'
      - 'segmentsdeparturetimeraw'
      - 'segmentsarrivaltimeepochseconds'
      - 'segmentsarrivaltimeraw'
      - 'segmentsarrivalairportcode'
      - 'segmentsdepartureairportcode'
      - 'segmentsairlinename'
      - 'segmentsairlinecode'
      - 'segmentsequipmentdescription'
      - 'segmentsdurationinseconds'
      - 'segmentsdistance'
      - 'segmentscabincode'
      - 'farebasiscode'      
  AIRPORT_PK:
    - 'startingairport'
  AIRLINE_PK:
    - 'segmentsairlinecode'
  LINK_FLIGHT_AIRPORT_PK:
    - 'legId'
    - 'startingairport'
  LINK_FLIGHT_AIRLINE_PK:
    - 'legId'
    - 'segmentsairlinecode'

{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}