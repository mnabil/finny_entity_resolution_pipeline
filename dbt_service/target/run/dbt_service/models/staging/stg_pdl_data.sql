
  create view "finny_db"."public_staging"."stg_pdl_data__dbt_tmp"
    
    
  as (
    -- Staging model for PDL data
-- This model cleans and normalizes the raw PDL data with location parsing



select
    pdl_id,
    name,
    email,
    company,
    company_revenue,
    title,
    location,
    -- Extract city (usually the first part before comma, if comma exists)
    case 
        when location like '%,%' then trim(split_part(location, ',', 1))
        else null
    end as city,
    -- Extract and standardize state using ISO mapping
    coalesce(
        sim.iso_code, 
        case 
            when location like '%,%' then trim(split_part(location, ',', -1))
            else trim(location)
        end
    ) as state
from "finny_db"."public_raw"."raw_pdl_data" pdl
left join "finny_db"."public"."state_iso_mapping" sim
    on upper(
        case 
            when pdl.location like '%,%' then trim(split_part(pdl.location, ',', -1))
            else trim(pdl.location)
        end
    ) = upper(sim.state_name)
where pdl_id is not null
  );