-- Payment type dimension.
--
-- The trip data stores payment_type as a number. The meaning of those numbers
-- lives in the TLC data dictionary, not in the data itself.
--
-- Encoding it here is the point: business meaning belongs in the warehouse,
-- version-controlled and reviewable, not in a spreadsheet on somebody's laptop
-- or memorized by one analyst.

with payment_types as (

    select * from (
        values
            (0, 'Flex Fare',    false),
            (1, 'Credit card',  true),
            (2, 'Cash',         false),
            (3, 'No charge',    false),
            (4, 'Dispute',      false),
            (5, 'Unknown',      false),
            (6, 'Voided trip',  false)
    ) as t (payment_type_id, payment_type_name, is_card_payment)

)

select
    payment_type_id,
    payment_type_name,
    is_card_payment,

    -- Only card payments record tips reliably. Analysts who miss this compute
    -- a tip rate across all payments and report a number that is badly wrong.
    is_card_payment as tips_are_reliable

from payment_types

union all

select -1, 'Unknown', false, false
