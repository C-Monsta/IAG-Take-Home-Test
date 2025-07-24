{{ config(materialized='view') }}

SELECT *
FROM {{ source('iag', 'policy_events') }}