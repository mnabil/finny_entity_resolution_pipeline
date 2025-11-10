-- Staging model for FXF data
-- This model cleans and normalizes the raw FXF data with location parsing

{{ config(materialized='view') }}

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
from {{ ref('raw_fxf_data') }} fxf
left join {{ ref('state_iso_mapping') }} sim
    on upper(
        case 
            when fxf.location like '%,%' then trim(split_part(fxf.location, ',', -1))
            else trim(fxf.location)
        end
    ) = upper(sim.state_name)
where fxf_id is not null