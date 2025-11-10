-- City and State profiling analysis for staging data
-- This model analyzes geographic distribution patterns using parsed city and state fields

{{ config(materialized='table') }}

with fxf_city_state_profile as (
    select
        'stg_fxf_data' as source_table,
        'staging' as data_layer,
        city,
        state,
        location,
        count(*) as contact_count,
        count(distinct fxf_id) as unique_contacts,
        count(distinct company) as company_count,
        count(distinct title) as title_count,
        count(company_revenue) as contacts_with_revenue,
        avg(company_revenue) as avg_revenue,
        min(company_revenue) as min_revenue,
        max(company_revenue) as max_revenue,
        -- Staging-specific metrics
        round(100.0 * count(email) / count(*), 2) as email_completeness_pct,
        round(100.0 * count(company_revenue) / count(*), 2) as revenue_completeness_pct,
        count(*) filter (where email is not null and email like '%@%') as valid_email_count,
        round(100.0 * count(*) filter (where email is not null and email like '%@%') / count(*), 2) as email_validity_pct
    from {{ ref('stg_fxf_data') }}
    where location is not null and trim(location) != ''
    group by city, state, location
),

pdl_city_state_profile as (
    select
        'stg_pdl_data' as source_table,
        'staging' as data_layer,
        city,
        state,
        location,
        count(*) as contact_count,
        count(distinct pdl_id) as unique_contacts,
        count(distinct company) as company_count,
        count(distinct title) as title_count,
        count(company_revenue) as contacts_with_revenue,
        avg(company_revenue) as avg_revenue,
        min(company_revenue) as min_revenue,
        max(company_revenue) as max_revenue,
        -- Staging-specific metrics
        round(100.0 * count(email) / count(*), 2) as email_completeness_pct,
        round(100.0 * count(company_revenue) / count(*), 2) as revenue_completeness_pct,
        count(*) filter (where email is not null and email like '%@%') as valid_email_count,
        round(100.0 * count(*) filter (where email is not null and email like '%@%') / count(*), 2) as email_validity_pct
    from {{ ref('stg_pdl_data') }}
    where location is not null and trim(location) != ''
    group by city, state, location
)

select * from fxf_city_state_profile
union all
select * from pdl_city_state_profile
order by contact_count desc