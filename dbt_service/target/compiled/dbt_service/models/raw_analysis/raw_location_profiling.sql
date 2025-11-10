-- Location profiling analysis for raw data with ISO code standardization via seed
-- This model analyzes geographic distribution patterns in the raw data



with fxf_location_profile as (
    select
        'fxf_data' as source_table,
        'raw' as data_layer,
        location,
        -- Extract state/region (usually the last part after comma, or the whole string)
        case 
            when location like '%,%' then trim(split_part(location, ',', -1))  -- Get the last part after comma
            else trim(location)
        end as state_region_raw,
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
        max(company_revenue) as max_revenue
    from "finny_db"."public_raw"."raw_fxf_data"
    where location is not null and trim(location) != ''
    group by location
),

fxf_location_standardized as (
    select 
        flp.*,
        coalesce(sim.iso_code, flp.state_region_raw) as state_region
    from fxf_location_profile flp
    left join "finny_db"."public"."state_iso_mapping" sim
        on upper(flp.state_region_raw) = upper(sim.state_name)
),

pdl_location_profile as (
    select
        'pdl_data' as source_table,
        'raw' as data_layer,
        location,
        -- Extract state/region (usually the last part after comma, or the whole string)
        case 
            when location like '%,%' then trim(split_part(location, ',', -1))  -- Get the last part after comma
            else trim(location)
        end as state_region_raw,
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
        max(company_revenue) as max_revenue
    from "finny_db"."public_raw"."raw_pdl_data"
    where location is not null and trim(location) != ''
    group by location
),

pdl_location_standardized as (
    select 
        plp.*,
        coalesce(sim.iso_code, plp.state_region_raw) as state_region
    from pdl_location_profile plp
    left join "finny_db"."public"."state_iso_mapping" sim
        on upper(plp.state_region_raw) = upper(sim.state_name)
),

combined_location_profile as (
    select
        'combined_raw' as source_table,
        'raw' as data_layer,
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
        null::text as state_region_raw
    from (
        select 
            source_table, data_layer, location, state_region, city, contact_count, unique_contacts,
            company_count, title_count, contacts_with_revenue, avg_revenue, min_revenue, max_revenue
        from fxf_location_standardized
        union all
        select 
            source_table, data_layer, location, state_region, city, contact_count, unique_contacts,
            company_count, title_count, contacts_with_revenue, avg_revenue, min_revenue, max_revenue
        from pdl_location_standardized
    ) combined
    group by location, state_region, city
)

select 
    source_table, data_layer, location, state_region, city, contact_count, unique_contacts,
    company_count, title_count, contacts_with_revenue, avg_revenue, min_revenue, max_revenue, state_region_raw
from fxf_location_standardized
union all
select 
    source_table, data_layer, location, state_region, city, contact_count, unique_contacts,
    company_count, title_count, contacts_with_revenue, avg_revenue, min_revenue, max_revenue, state_region_raw
from pdl_location_standardized
union all  
select 
    source_table, data_layer, location, state_region, city, contact_count, unique_contacts,
    company_count, title_count, contacts_with_revenue, avg_revenue, min_revenue, max_revenue, state_region_raw
from combined_location_profile
order by contact_count desc