{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_id',
        on_schema_change = 'fail'
    )
}}

-- The fact table. This is the center of the star.
--
-- GRAIN: one row per completed taxi trip. Write the grain down before writing
-- any SQL — "what does one row mean?" is the first question of dimensional
-- modeling and the first question a good interviewer asks.
--
-- A fact table holds:
--   - foreign keys pointing at dimensions (date, location, payment type)
--   - measures: numbers you add up (fare, tip, distance, duration)
-- and nothing else. Descriptive text lives in dimensions, not here.
--
-- INCREMENTAL: materialized='incremental' means dbt builds the whole table the
-- first time, then only processes NEW rows on later runs. On a full rebuild
-- you would reprocess every row every night, which stops being viable fast.

with trips as (

    select * from {{ ref('stg_yellow_trips') }}

    {% if is_incremental() %}
    -- This block only runs when the table already exists. {{ this }} refers
    -- to the current table, so we ask it for its newest trip and take only
    -- what came after.
    where pickup_at > (
        select coalesce(max(pickup_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

)

select
    -- Primary key
    trip_id,

    -- Foreign keys into the dimensions. coalesce to -1 routes unmatched IDs
    -- to the 'Unknown' row instead of dropping them from every report.
    to_char(pickup_at, 'YYYYMMDD')::int              as pickup_date_key,
    coalesce(pickup_location_id, -1)                 as pickup_location_id,
    coalesce(dropoff_location_id, -1)                as dropoff_location_id,
    coalesce(payment_type_id, -1)                    as payment_type_id,
    vendor_id,

    -- Degenerate dimensions: timestamps that are useful on the fact itself.
    pickup_at,
    dropoff_at,

    -- Measures. Everything below this line is additive — you can sum it
    -- across any combination of dimensions and get a meaningful number.
    passenger_count,
    trip_distance_miles,
    round(trip_duration_minutes::numeric, 2)         as trip_duration_minutes,
    fare_amount,
    extra_charges,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,

    -- Derived measures. Defining them once here means every dashboard agrees.
    -- Two teams computing "average speed" separately is how you end up with
    -- two different numbers in the same meeting.
    case
        when trip_duration_minutes > 0
        then round((trip_distance_miles / (trip_duration_minutes / 60.0))::numeric, 2)
    end                                              as avg_speed_mph,

    case
        when fare_amount > 0
        then round((tip_amount / fare_amount * 100)::numeric, 2)
    end                                              as tip_percentage

from trips
