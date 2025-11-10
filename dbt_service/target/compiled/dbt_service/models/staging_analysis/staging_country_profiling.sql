-- Country profiling analysis for staging data
-- This model analyzes geographic distribution patterns in the cleaned staging data



with fxf_country_profile as (
    select
        'stg_fxf_data' as source_table,
        'staging' as data_layer,
        -- Extract country from location (assuming format like "City, State" or "City, Country")
        case 
            when location like '%,%' then trim(split_part(location, ',', -1))
            else location
        end as country,
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
    from "finny_db"."public_staging"."stg_fxf_data"
    where location is not null and trim(location) != ''
    group by country
),

pdl_country_profile as (
    select
        'stg_pdl_data' as source_table,
        'staging' as data_layer,
        -- Extract country from location
        case 
            when location like '%,%' then trim(split_part(location, ',', -1))
            else location
        end as country,
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
    from "finny_db"."public_staging"."stg_pdl_data"
    where location is not null and trim(location) != ''
    group by country
)

select * from fxf_country_profile
union all
select * from pdl_country_profile
order by contact_count desc