-- Location dimension.
--
-- The trip data only stores location IDs (a number like 132). Nobody wants a
-- dashboard that says "top pickup zone: 132". This dimension turns that number
-- into "JFK Airport, Queens".
--
-- That is the whole job of a dimension: give the facts human context.
--
-- Source is a seed — a small reference CSV committed to the repo and loaded
-- with `dbt seed`. Seeds are for lookup data that rarely changes and does not
-- come from a source system.
--
-- NOTE ON QUOTING: the seed CSV's header is "LocationID,Borough,Zone,service_zone"
-- with capital letters, and dbt preserves that case when it creates the table.
-- Postgres folds UNQUOTED identifiers to lowercase, so locationid would not
-- match "LocationID". Double quotes preserve the exact case.
--
-- This is a real and very common source of bugs. Mixed-case column names are
-- worth normalising to lowercase as early as possible.

with zones as (

    select
        "LocationID"::int  as location_id,
        "Borough"          as borough,
        "Zone"             as zone_name,
        "service_zone"     as service_zone
    from {{ ref('taxi_zone_lookup') }}

)

select
    location_id,
    coalesce(borough, 'Unknown')       as borough,
    coalesce(zone_name, 'Unknown')     as zone_name,
    coalesce(service_zone, 'Unknown')  as service_zone,

    -- A convenience column so dashboards do not have to concatenate.
    coalesce(zone_name, 'Unknown') || ', ' || coalesce(borough, 'Unknown')
                                       as zone_full_name,

    -- Flag airport zones. Business questions about airport traffic are common
    -- enough to be worth encoding once here.
    case when coalesce(zone_name, '') in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')
         then true else false
    end                                as is_airport

from zones

union all

-- Every dimension needs a fallback row. When a fact references a location ID
-- that does not exist in the lookup, it joins to this instead of vanishing
-- from your reports. Losing rows silently in a join is one of the most common
-- and most damaging warehouse bugs.
select
    -1        as location_id,
    'Unknown' as borough,
    'Unknown' as zone_name,
    'Unknown' as service_zone,
    'Unknown' as zone_full_name,
    false     as is_airport
