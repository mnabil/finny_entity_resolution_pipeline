-- Company profiling analysis for staging data with structured location data
-- This model analyzes company distribution patterns in the cleaned staging data

{{ config(materialized='table') }}

with fxf_company_profile as (
    select
        'stg_fxf_data' as source_table,
        'staging' as data_layer,
        company,
        count(*) as employee_count,
        count(distinct fxf_id) as unique_employees,
        count(distinct title) as unique_titles,
        count(distinct location) as office_locations,
        count(distinct city) as unique_cities,
        count(distinct state) as unique_states,
        count(company_revenue) as employees_with_revenue_info,
        count(*) - count(company_revenue) as employees_missing_revenue,
        avg(company_revenue) as avg_revenue,
        min(company_revenue) as min_revenue,
        max(company_revenue) as max_revenue,
        -- Calculate data completeness and quality percentages
        round(100.0 * count(name) / count(*), 2) as name_completeness_pct,
        round(100.0 * count(email) / count(*), 2) as email_completeness_pct,
        round(100.0 * count(title) / count(*), 2) as title_completeness_pct,
        round(100.0 * count(location) / count(*), 2) as location_completeness_pct,
        round(100.0 * count(city) / count(*), 2) as city_completeness_pct,
        round(100.0 * count(state) / count(*), 2) as state_completeness_pct,
        -- Staging-specific quality metrics
        count(*) filter (where email is not null and email like '%@%') as valid_email_count,
        round(100.0 * count(*) filter (where email is not null and email like '%@%') / count(*), 2) as email_validity_pct,
        count(distinct case when email like '%@%' then split_part(email, '@', 2) end) as email_domain_count
    from {{ ref('stg_fxf_data') }}
    where company is not null and trim(company) != ''
    group by company
),

pdl_company_profile as (
    select
        'stg_pdl_data' as source_table,
        'staging' as data_layer,
        company,
        count(*) as employee_count,
        count(distinct pdl_id) as unique_employees,
        count(distinct title) as unique_titles,
        count(distinct location) as office_locations,
        count(distinct city) as unique_cities,
        count(distinct state) as unique_states,
        count(company_revenue) as employees_with_revenue_info,
        count(*) - count(company_revenue) as employees_missing_revenue,
        avg(company_revenue) as avg_revenue,
        min(company_revenue) as min_revenue,
        max(company_revenue) as max_revenue,
        -- Calculate data completeness and quality percentages
        round(100.0 * count(name) / count(*), 2) as name_completeness_pct,
        round(100.0 * count(email) / count(*), 2) as email_completeness_pct,
        round(100.0 * count(title) / count(*), 2) as title_completeness_pct,
        round(100.0 * count(location) / count(*), 2) as location_completeness_pct,
        round(100.0 * count(city) / count(*), 2) as city_completeness_pct,
        round(100.0 * count(state) / count(*), 2) as state_completeness_pct,
        -- Staging-specific quality metrics
        count(*) filter (where email is not null and email like '%@%') as valid_email_count,
        round(100.0 * count(*) filter (where email is not null and email like '%@%') / count(*), 2) as email_validity_pct,
        count(distinct case when email like '%@%' then split_part(email, '@', 2) end) as email_domain_count
    from {{ ref('stg_pdl_data') }}
    where company is not null and trim(company) != ''
    group by company
)

select * from fxf_company_profile
union all
select * from pdl_company_profile
order by employee_count desc