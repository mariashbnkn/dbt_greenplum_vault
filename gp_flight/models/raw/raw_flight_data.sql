{{
    config(
        schema='raw',
        materialized='table',
		appendonly='true',
		orientation='column',
        compresstype='zlib',
        compresslevel=4,
        blocksize=32768
    )
}}


with flight_day_dedup as (
		select * from (
			select *,
				   row_number() over (
				   	   partition by fl."legid"
		               order by fl."searchdate" asc
		           ) as rn
			from {{ source('test', 'flight_data') }} fl
			where 
				'{{ var('raw_flight_data')['start_date'] }}' <= searchdate
				and 
				searchdate < '{{ var('raw_flight_data')['end_date'] }}'
		) as h
		where rn = 1
	)
select
	legId,
    searchDate,
    flightDate,
    startingAirport,
    destinationAirport,
    fareBasisCode,
    travelDuration,
    elapsedDays,
    isBasicEconomy,
    isRefundable,
    isNonStop,
    baseFare,
    totalFare,
    seatsRemaining,
    totalTravelDistance,
    segmentsDepartureTimeEpochSeconds,
    segmentsDepartureTimeRaw,
    segmentsArrivalTimeEpochSeconds,
    segmentsArrivalTimeRaw,
    segmentsArrivalAirportCode,
    segmentsDepartureAirportCode,
    segmentsAirlineName,
    segmentsAirlineCode,
    segmentsEquipmentDescription,
    segmentsDurationInSeconds,
    segmentsDistance,
    segmentsCabinCode,
	'TEST_FLIGHT_DATA' as record_source
from flight_day_dedup ra