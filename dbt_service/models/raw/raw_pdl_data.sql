-- Raw PDL data from seed  
-- This model creates a table in the raw schema from our CSV data

{{ config(materialized='table') }}

select * from {{ ref('pdl_data') }}