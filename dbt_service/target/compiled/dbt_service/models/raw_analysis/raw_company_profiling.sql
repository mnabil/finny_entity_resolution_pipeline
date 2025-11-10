-- Company profiling analysis for raw data
-- This model analyzes company distribution patterns in the raw data layer



with fxf_company_profile as (
    select
        'fxf_data' as source_table,
        'raw' as data_layer,
        company,
        count(*) as employee_count,
        count(distinct fxf_id) as unique_employees,
        count(distinct title) as unique_titles,
        count(distinct location) as office_locations,
        count(company_revenue) as employees_with_revenue_info,
        count(*) - count(company_revenue) as employees_missing_revenue,
        avg(company_revenue) as avg_revenue,
        min(company_revenue) as min_revenue,
        max(company_revenue) as max_revenue,
        -- Calculate data completeness percentages
        round(100.0 * count(name) / count(*), 2) as name_completeness_pct,
        round(100.0 * count(email) / count(*), 2) as email_completeness_pct,
        round(100.0 * count(title) / count(*), 2) as title_completeness_pct,
        round(100.0 * count(location) / count(*), 2) as location_completeness_pct
    from "finny_db"."public_raw"."raw_fxf_data"
    where company is not null and trim(company) != ''
    group by company
),

pdl_company_profile as (
    select
        'pdl_data' as source_table,
        'raw' as data_layer,
        company,
        count(*) as employee_count,
        count(distinct pdl_id) as unique_employees,
        count(distinct title) as unique_titles,
        count(distinct location) as office_locations,
        count(company_revenue) as employees_with_revenue_info,
        count(*) - count(company_revenue) as employees_missing_revenue,
        avg(company_revenue) as avg_revenue,
        min(company_revenue) as min_revenue,
        max(company_revenue) as max_revenue,
        -- Calculate data completeness percentages
        round(100.0 * count(name) / count(*), 2) as name_completeness_pct,
        round(100.0 * count(email) / count(*), 2) as email_completeness_pct,
        round(100.0 * count(title) / count(*), 2) as title_completeness_pct,
        round(100.0 * count(location) / count(*), 2) as location_completeness_pct
    from "finny_db"."public_raw"."raw_pdl_data"
    where company is not null and trim(company) != ''
    group by company
)

select * from fxf_company_profile
union all
select * from pdl_company_profile
order by employee_count desc