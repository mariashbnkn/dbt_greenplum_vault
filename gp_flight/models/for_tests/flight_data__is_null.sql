-- models/date_range.sql
{{
  config(
    materialized='table',
    compresstype='zlib'
  )
}}

select
	sum(case 
			when legid is null 
			or searchdate is null
			or flightdate is null
			or startingairport is null
			or destinationairport is null
			or farebasiscode is null
			or travelduration is null
			or elapseddays is null
			or isbasiceconomy is null
			or isrefundable is null
			or isnonstop is null
			or basefare is null
			or totalfare is null
			or seatsremaining is null
			or totaltraveldistance is null
			or segmentsdeparturetimeepochseconds is null
			or segmentsdeparturetimeraw is null
			or segmentsarrivaltimeepochseconds is null
			or segmentsarrivaltimeraw is null
			or segmentsarrivalairportcode is null
			or segmentsdepartureairportcode is null
			or segmentsairlinename is null
			or segmentsairlinecode is null
			or segmentsequipmentdescription is null
			or segmentsdurationinseconds is null
			or segmentsdistance is null
			or segmentscabincode is null
		then 1 else 0 
		end) as is_null
from {{ source('test', 'flight_data') }}