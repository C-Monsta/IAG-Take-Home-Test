{{ config(
    materialized='view',
    catalog = 'iag_cat',
    schema='iag'
) }}


--------------------------------------------------------------------------------------------------------
-- Policy_Lifecycle_Model
-- By Carol Moynham
-- V1 20250721 -- Initial model
-- Summary one line per policy, includes metrics like number of quotes, average premium
-- based mainly on iag_cat.iag.policy_events and  iag_cat.iag.quote_bind
---------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------
-- Steps
-- Take details for most recent quotes and binds for summary table
-- Collate data in summary
---------------------------------------------------------------------------------------------------------


with most_recent_bind as (
    select *
    from (
            select policy_id,
                event_date,
                event_type,
                channel,
                state,
                premium_amount,
                row_number() over (partition by policy_id order by event_date desc) as rn
            -- from iag_cat.iag.policy_events pe
            from {{ ref('src_policy_events') }} pe
            where upper(event_type) = 'BIND'
            )
    where rn = 1
    ),

    most_recent_quote as (
    select *
    from (
            select policy_id,
                event_date,
                event_type,
                channel,
                state,
                premium_amount,
                row_number() over (partition by policy_id order by event_date desc) as rn
            -- from iag_cat.iag.policy_events pe
            from {{ ref('src_policy_events') }} pe
            where upper(event_type) = 'QUOTE'
            )
    where rn = 1    
    )

-----------------------------------------------------------------------------------------------
-- Final Summary
-----------------------------------------------------------------------------------------------

select pe.policy_id,
count(distinct pe.customer_id) as customers_on_policy,
max(mrq.premium_amount) as most_recent_quote_premium, --selecting max to remove nulls
max(mrq.channel) as most_recent_quote_channel,
max(mrq.state) as most_recent_quote_state,
max(mrb.premium_amount) as most_recent_bind_premium,
max(mrb.channel) as most_recent_bind_channel,
max(mrb.state) as most_recent_bind_state,
count(distinct case when upper(pe.event_type) = 'QUOTE' THEN pe.event_date end) as quotes,
count(distinct case when upper(pe.event_type) = 'BIND' THEN pe.event_date end) as binds,
count(distinct case when upper(pe.event_type) = 'CANCEL' THEN pe.event_date end) as cancels,
avg(coalesce(case when upper(pe.event_type) = 'QUOTE' then pe.premium_amount end, 0)) as avg_quote_premium,
avg(coalesce(case when upper(pe.event_type) = 'BIND' then pe.premium_amount end, 0)) as avg_bind_premium,
avg(coalesce(qb.days_between_quote_and_bind, 0)) as avg_days_quote_bind

-- from iag_cat.iag.policy_events pe
from {{ ref('src_policy_events') }} pe

-- left join iag_cat.iag.quote_bind qb
left join {{ ref('quote_bind') }} qb
on pe.policy_id = qb.bind_policy

left join most_recent_bind mrb
on mrb.policy_id = pe.policy_id
and mrb.event_date = pe.event_date
and mrb.event_type = pe.event_type

left join most_recent_quote mrq
on mrq.policy_id = pe.policy_id
and mrq.event_date = pe.event_date
and mrq.event_type = pe.event_type

-- where pe.policy_id = 1010 --TESTING
group by all