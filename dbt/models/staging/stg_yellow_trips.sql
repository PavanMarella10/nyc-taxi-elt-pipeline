-- Staging model: clean up raw data, one concern at a time.
--
-- A staging model does exactly four jobs and nothing else:
--   1. rename columns to something a human understands
--   2. cast types
--   3. deduplicate
--   4. drop rows that cannot possibly be real
--
-- It does NOT join to other tables and does NOT aggregate. Keeping staging
-- boring is what makes the layers above it easy to reason about.

with source as (

    select * from {{ source('raw', 'yellow_trips') }}

),

renamed as (

    select
        -- A surrogate key: a hash of the fields that together identify a
        -- trip. The source has no trip ID, so we build a stable one. Same
        -- trip in, same key out, every run.
        md5(
            coalesce(vendorid::text, '')               || '|' ||
            coalesce(tpep_pickup_datetime::text, '')   || '|' ||
            coalesce(tpep_dropoff_datetime::text, '')  || '|' ||
            coalesce(pulocationid::text, '')           || '|' ||
            coalesce(dolocationid::text, '')           || '|' ||
            coalesce(total_amount::text, '')
        ) as trip_id,

        vendorid                        as vendor_id,
        tpep_pickup_datetime            as pickup_at,
        tpep_dropoff_datetime           as dropoff_at,
        pulocationid                    as pickup_location_id,
        dolocationid                    as dropoff_location_id,
        payment_type                    as payment_type_id,
        ratecodeid::int                 as rate_code_id,

        passenger_count::int            as passenger_count,
        trip_distance                   as trip_distance_miles,

        fare_amount,
        extra                           as extra_charges,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,

        case when store_and_fwd_flag = 'Y' then true
             when store_and_fwd_flag = 'N' then false
        end                             as was_store_and_forward,

        -- Derived measure: how long the trip took. Computing it once here
        -- means every downstream model uses the same definition.
        extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime))
            / 60.0                      as trip_duration_minutes

    from source

),

deduplicated as (

    -- Real feeds send the same record twice. row_number() ranks rows within
    -- each key group; keeping rank 1 keeps exactly one copy.
    --
    -- This pattern comes up in interviews constantly. Know it cold.
    select *
    from (
        select
            *,
            row_number() over (
                partition by trip_id
                order by pickup_at
            ) as _row_num
        from renamed
    ) ranked
    where _row_num = 1

),

cleaned as (

    select *
    from deduplicated
    where
        -- Trips must move forward in time
        dropoff_at > pickup_at

        -- and fall inside the window we actually loaded. Source files contain
        -- stray records dated years away — you saw these in step 2.
        and pickup_at >= '{{ var("start_date") }}'
        and pickup_at <  '{{ var("end_date") }}'

        -- Money cannot be negative
        and fare_amount >= 0
        and total_amount >= 0

        -- A taxi trip is more than zero miles and less than a road trip
        and trip_distance_miles > 0
        and trip_distance_miles < 200

        -- and shorter than a full day
        and trip_duration_minutes between 1 and 1440

)

select
    trip_id,
    vendor_id,
    pickup_at,
    dropoff_at,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,
    rate_code_id,
    passenger_count,
    trip_distance_miles,
    trip_duration_minutes,
    fare_amount,
    extra_charges,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    was_store_and_forward
from cleaned
