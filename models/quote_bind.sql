{{ config(
    materialized='view',
    catalog = 'iag_cat',
    schema='iag'
) }}

--------------------------------------------------------------------------------------------------------
-- Quote and bind view
-- Shows every quote/ bind pattern with additional policy details
-- Note: Additional details are based on the bind details, and may vary between quotes
---------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------
-- Steps
-- Get all policies that have a quote/ bind sequence
-- Group them into segments
-- Attach policy detials to show quote/ bind times and additional details
----------------------------------------------------------------------------------------------------------



-- list of all policies with both a quote and a bind value
-- used to group quotes and bind segments
with quotes as (
            select evq.policy_id,
            evq.event_type,
            evq.event_date
--          from iag_cat.iag.policy_events evq
            from {{ ref('src_policy_events') }} evq
            where upper(evq.event_type) = 'QUOTE'
),

bind_with_quotes as (
    select evb.policy_id
    , evb.event_type
    , evb.event_date
    , evb.channel
    , evb.state
    , evb.premium_amount
--  from iag_cat.iag.policy_events evb
    from {{ ref('src_policy_events') }} evb
    inner join quotes evq
    on evq.policy_id = evb.policy_id
    and evb.event_date > evq.event_date --only want quotes before binds
    where upper(evb.event_type) = 'BIND'

),

/*
-- TESTING
select *
from bind_with_quotes
-- order by policy_id
where policy_id = 1007
*/

quote_bind as (
select a.*
from (
select bwq.policy_id, bwq.event_type, bwq.event_date
from bind_with_quotes bwq
UNION ALL
select evq.policy_id, evq.event_type, evq.event_date
from quotes evq
inner join bind_with_quotes bwq
on bwq.policy_id = evq.policy_id
and bwq.event_date > evq.event_date
) a
order by policy_id, event_date
),

-- rank to segment quote-bind sequences
group_reset AS (
  SELECT
    qb.policy_id,
    qb.event_type,
    qb.event_date,
    bwq.state,
    bwq.channel,
    bwq.premium_amount,
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY qb.policy_id ORDER BY qb.event_date) = 1 THEN 1
      WHEN UPPER(qb.event_type) = 'QUOTE'
           AND (
             LAG(UPPER(qb.event_type)) OVER (PARTITION BY qb.policy_id ORDER BY qb.event_date) IS NULL
             OR LAG(UPPER(qb.event_type)) OVER (PARTITION BY qb.policy_id ORDER BY qb.event_date) != 'QUOTE'
           )
      THEN 1
      ELSE 0
    END AS is_new_group
  FROM quote_bind qb

  LEFT JOIN bind_with_quotes bwq
  on qb.policy_id = bwq.policy_id
  and qb.event_date = bwq.event_date
  and qb.event_type = bwq.event_type

),

/*
-- TESTING
select policy_id, event_type, event_date, is_new_group
from group_reset
order by policy_id, event_date
*/

-- Grouped quote/ bind sequences
-- Used to find time between all quote and bind sequences
-- Only include channel and other metrics for bound policies, not quotes
-- Assumption - in summary table we will report on bound details only
sequence_group as (
  select distinct policy_id,
         event_type,
         event_date,
         channel,
         premium_amount,
         state,
    SUM(is_new_group) OVER (ORDER BY policy_id, event_date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
    AS sequence_group
  FROM group_reset
  order by policy_id, event_date
),

/*
-- TESTING
select *
from sequence_group
*/

-- earliest quote and bind by sequence
-- for summary table
min_quote_by_seq (
  select sequence_group
  , min(event_date) as earliest_quote
  , count(event_date) as count_quotes
  from sequence_group
  where upper(event_type) = 'QUOTE'
  group by 1
),

min_bind_by_seq (
  select sequence_group
  , min(event_date) as earliest_bind
  from sequence_group
  where upper(event_type) = 'BIND'
  group by 1
)


select sg.sequence_group
, sg.policy_id as bind_policy
, eq.earliest_quote
, eq.count_quotes
, eb.earliest_bind
, datediff(eb.earliest_bind, eq.earliest_quote) as days_between_quote_and_bind
, max(sg.state) as bind_state
-- , sg.customer_id
-- , sg.channel
, max(sg.premium_amount) as bind_premium
from sequence_group sg
inner join min_quote_by_seq eq
on sg.sequence_group = eq.sequence_group
inner join min_bind_by_seq eb
on sg.sequence_group = eb.sequence_group
group by all
order by sg.sequence_group