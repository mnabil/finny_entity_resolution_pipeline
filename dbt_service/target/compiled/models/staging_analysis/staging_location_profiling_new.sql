-- Location profiling analysis for staging data
-- This model analyzes geographic distribution patterns in the cleaned staging data



with fxf_location_profile as (
    select
        'stg_fxf_data' as source_table,
        'staging' as data_layer,
        location,
        -- Extract state/region (usually the last part after comma, or the whole string)
        case 
            when location like '%,%' then trim(split_part(location, ',', -1))  -- Get the last part after comma
            else trim(location)
        end as state_region,
        -- Extract city (usually the first part before comma, if comma exists)
        case 
            when location like '%,%' then trim(split_part(location, ',', 1))  -- Get the first part before comma
            else null
        end as city,
        count(*) as contact_count,
        count(distinct fxf_id) as unique_contacts,
        count(distinct company) as company_count,
        count(distinct title) as title_count,
        count(company_revenue) as contacts_with_revenue,
        avg(company_revenue) as avg_revenue,
        min(company_revenue) as min_revenue,
        max(company_revenue) as max_revenue,
        -- Staging-specific quality metrics
        count(*) filter (where name is not null and trim(name) != '') as contacts_with_name,
        count(*) filter (where email is not null and email like '%@%') as contacts_with_valid_email,
        round(100.0 * count(*) filter (where email is not null and email like '%@%') / count(*), 2) as email_validity_pct
    from "finny_db"."public_staging"."stg_fxf_data"
    where location is not null and trim(location) != ''
    group by location
),

pdl_location_profile as (
    select
        'stg_pdl_data' as source_table,
        'staging' as data_layer,
        location,
        -- Extract state/region (usually the last part after comma, or the whole string)
        case 
            when location like '%,%' then trim(split_part(location, ',', -1))  -- Get the last part after comma
            else trim(location)
        end as state_region,
        -- Extract city (usually the first part before comma, if comma exists)
        case 
            when location like '%,%' then trim(split_part(location, ',', 1))  -- Get the first part before comma
            else null
        end as city,
        count(*) as contact_count,
        count(distinct pdl_id) as unique_contacts,
        count(distinct company) as company_count,
        count(distinct title) as title_count,
        count(company_revenue) as contacts_with_revenue,
        avg(company_revenue) as avg_revenue,
        min(company_revenue) as min_revenue,
        max(company_revenue) as max_revenue,
        -- Staging-specific quality metrics
        count(*) filter (where name is not null and trim(name) != '') as contacts_with_name,
        count(*) filter (where email is not null and email like '%@%') as contacts_with_valid_email,
        round(100.0 * count(*) filter (where email is not null and email like '%@%') / count(*), 2) as email_validity_pct
    from "finny_db"."public_staging"."stg_pdl_data"
    where location is not null and trim(location) != ''
    group by location
),

combined_location_profile as (
    select
        'combined_staging' as source_table,
        'staging' as data_layer,
        location,
        state_region,
        city,
        sum(contact_count) as contact_count,
        sum(unique_contacts) as unique_contacts,
        sum(company_count) as company_count,
        sum(title_count) as title_count,
        sum(contacts_with_revenue) as contacts_with_revenue,
        avg(avg_revenue) as avg_revenue,
        min(min_revenue) as min_revenue,
        max(max_revenue) as max_revenue,
        sum(contacts_with_name) as contacts_with_name,
        sum(contacts_with_valid_email) as contacts_with_valid_email,
        round(100.0 * sum(contacts_with_valid_email) / sum(contact_count), 2) as email_validity_pct
    from (
        select * from fxf_location_profile
        union all
        select * from pdl_location_profile
    ) combined
    group by location, state_region, city
)

select * from fxf_location_profile
union all
select * from pdl_location_profile
union all  
select * from combined_location_profile
order by contact_count desc