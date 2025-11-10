
  create view "finny_db"."public_staging"."stg_fxf_data__dbt_tmp"
    
    
  as (
    -- Staging model for FXF data
-- This model cleans and normalizes the raw FXF data with location parsing



select
    fxf_id,
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
from "finny_db"."public_raw"."raw_fxf_data" fxf
left join "finny_db"."public"."state_iso_mapping" sim
    on upper(
        case 
            when fxf.location like '%,%' then trim(split_part(fxf.location, ',', -1))
            else trim(fxf.location)
        end
    ) = upper(sim.state_name)
where fxf_id is not null
  );