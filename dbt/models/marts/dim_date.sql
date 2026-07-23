-- Date dimension.
--
-- Every warehouse has one. It is generated, not loaded — no source system
-- sends you a list of dates.
--
-- Why bother? Without it, "show me weekend revenue by quarter" means writing
-- date arithmetic in every single query, and everyone writes it slightly
-- differently. With it, that logic lives in one place and analysts just join
-- and filter on plain columns.

with date_spine as (

    -- generate_series builds one row per day between two dates.
    select generate_series(
        '{{ var("start_date") }}'::date,
        ('{{ var("end_date") }}'::date - interval '1 day')::date,
        interval '1 day'
    )::date as date_day

)

select
    -- A date key in YYYYMMDD form. Integer keys are compact and join fast,
    -- and unlike a raw date they are readable at a glance: 20240115.
    to_char(date_day, 'YYYYMMDD')::int    as date_key,

    date_day,
    extract(year    from date_day)::int   as year,
    extract(quarter from date_day)::int   as quarter,
    extract(month   from date_day)::int   as month,
    trim(to_char(date_day, 'Month'))      as month_name,
    extract(day     from date_day)::int   as day_of_month,

    -- Postgres numbers Sunday as 0. Shifting to 1-7 with Monday first matches
    -- how most businesses think about a week.
    (extract(isodow from date_day))::int  as day_of_week,
    trim(to_char(date_day, 'Day'))        as day_name,

    extract(isodow from date_day) in (6, 7) as is_weekend,
    extract(week from date_day)::int      as week_of_year

from date_spine
