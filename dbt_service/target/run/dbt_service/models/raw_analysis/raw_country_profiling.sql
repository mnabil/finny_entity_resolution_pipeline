
  
    

  create  table "finny_db"."public_raw_analysis"."raw_country_profiling__dbt_tmp"
  
  
    as
  
  (
    -- Country profiling analysis for raw data
-- This model analyzes geographic distribution patterns in the raw data layer



with fxf_country_profile as (
    select
        'fxf_data' as source_table,
        'raw' as data_layer,
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
        max(company_revenue) as max_revenue
    from "finny_db"."public_raw"."raw_fxf_data"
    where location is not null and trim(location) != ''
    group by country
),

pdl_country_profile as (
    select
        'pdl_data' as source_table,
        'raw' as data_layer,
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
        max(company_revenue) as max_revenue
    from "finny_db"."public_raw"."raw_pdl_data"
    where location is not null and trim(location) != ''
    group by country
),

combined_country_summary as (
    select
        'combined_raw' as source_table,
        'raw' as data_layer,
        country,
        sum(contact_count) as contact_count,
        sum(unique_contacts) as unique_contacts,
        sum(company_count) as company_count,
        sum(title_count) as title_count,
        sum(contacts_with_revenue) as contacts_with_revenue,
        avg(avg_revenue) as avg_revenue,
        min(min_revenue) as min_revenue,
        max(max_revenue) as max_revenue
    from (
        select * from fxf_country_profile
        union all
        select * from pdl_country_profile
    ) combined
    group by country
)

select * from fxf_country_profile
union all
select * from pdl_country_profile
union all
select * from combined_country_summary
order by contact_count desc
  );
  