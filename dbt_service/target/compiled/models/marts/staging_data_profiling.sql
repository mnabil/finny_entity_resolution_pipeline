-- Staging data profiling analysis
-- This model analyzes data quality patterns in the staging data layer



with fxf_staging_profile as (
    select
        'stg_fxf_data' as source_table,
        'staging' as data_layer,
        count(*) as total_records,
        count(distinct fxf_id) as unique_ids,
        count(*) - count(distinct fxf_id) as duplicate_ids,
        count(name) as non_null_names,
        count(*) - count(name) as null_names,
        count(email) as non_null_emails,
        count(*) - count(email) as null_emails,
        count(company) as non_null_companies,
        count(*) - count(company) as null_companies,
        count(company_revenue) as non_null_revenues,
        count(*) - count(company_revenue) as null_revenues,
        count(title) as non_null_titles,
        count(*) - count(title) as null_titles,
        count(location) as non_null_locations,
        count(*) - count(location) as null_locations,
        count(distinct company) as unique_companies,
        count(distinct location) as unique_locations,
        count(distinct title) as unique_titles,
        -- Staging-specific metrics
        round(100.0 * count(email) / count(*), 2) as email_completeness_pct,
        round(100.0 * count(company_revenue) / count(*), 2) as revenue_completeness_pct,
        round(avg(company_revenue)::numeric, 0) as avg_company_revenue,
        max(company_revenue) as max_company_revenue,
        min(company_revenue) as min_company_revenue
    from "finny_db"."public_staging"."stg_fxf_data"
),

pdl_staging_profile as (
    select
        'stg_pdl_data' as source_table,
        'staging' as data_layer,
        count(*) as total_records,
        count(distinct pdl_id) as unique_ids,
        count(*) - count(distinct pdl_id) as duplicate_ids,
        count(name) as non_null_names,
        count(*) - count(name) as null_names,
        count(email) as non_null_emails,
        count(*) - count(email) as null_emails,
        count(company) as non_null_companies,
        count(*) - count(company) as null_companies,
        count(company_revenue) as non_null_revenues,
        count(*) - count(company_revenue) as null_revenues,
        count(title) as non_null_titles,
        count(*) - count(title) as null_titles,
        count(location) as non_null_locations,
        count(*) - count(location) as null_locations,
        count(distinct company) as unique_companies,
        count(distinct location) as unique_locations,
        count(distinct title) as unique_titles,
        -- Staging-specific metrics
        round(100.0 * count(email) / count(*), 2) as email_completeness_pct,
        round(100.0 * count(company_revenue) / count(*), 2) as revenue_completeness_pct,
        round(avg(company_revenue)::numeric, 0) as avg_company_revenue,
        max(company_revenue) as max_company_revenue,
        min(company_revenue) as min_company_revenue
    from "finny_db"."public_staging"."stg_pdl_data"
),

data_quality_comparison as (
    select
        'data_quality_summary' as source_table,
        'staging' as data_layer,
        (select sum(total_records) from fxf_staging_profile union all select sum(total_records) from pdl_staging_profile) as total_records,
        0 as unique_ids,
        0 as duplicate_ids,
        0 as non_null_names,
        0 as null_names,
        (select sum(non_null_emails) from fxf_staging_profile union all select sum(non_null_emails) from pdl_staging_profile) as non_null_emails,
        (select sum(null_emails) from fxf_staging_profile union all select sum(null_emails) from pdl_staging_profile) as null_emails,
        0 as non_null_companies,
        0 as null_companies,
        0 as non_null_revenues,
        0 as null_revenues,
        0 as non_null_titles,
        0 as null_titles,
        0 as non_null_locations,
        0 as null_locations,
        0 as unique_companies,
        0 as unique_locations,
        0 as unique_titles,
        round(100.0 * (select sum(non_null_emails) from fxf_staging_profile union all select sum(non_null_emails) from pdl_staging_profile) / 
              (select sum(total_records) from fxf_staging_profile union all select sum(total_records) from pdl_staging_profile), 2) as email_completeness_pct,
        0 as revenue_completeness_pct,
        0 as avg_company_revenue,
        0 as max_company_revenue,
        0 as min_company_revenue
    limit 1
)

select * from fxf_staging_profile
union all
select * from pdl_staging_profile