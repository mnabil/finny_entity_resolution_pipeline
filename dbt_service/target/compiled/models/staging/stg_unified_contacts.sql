-- Unified prospect records from both FXF and PDL sources
-- This model combines both datasets into a single standardized format



with fxf_prospects as (
    select
        'fxf' as data_source,
        fxf_id as source_id,
        name,
        email,
        company,
        title,
        location,
        city,
        state,
        company_revenue,
        -- Add source-specific fields as nulls for consistency
        null as pdl_id
    from "finny_db"."public_staging"."stg_fxf_data"
),

pdl_prospects as (
    select
        'pdl' as data_source,
        pdl_id as source_id,
        name,
        email,
        company,
        title,
        location,
        city,
        state,
        company_revenue,
        -- Add source-specific fields as nulls for consistency
        null as fxf_id
    from "finny_db"."public_staging"."stg_pdl_data"
),

unified_prospects as (
    select
        row_number() over (order by data_source, source_id) as prospect_id,
        data_source,
        source_id,
        case when data_source = 'fxf' then source_id else null end as fxf_id,
        case when data_source = 'pdl' then source_id else null end as pdl_id,
        name,
        email,
        company,
        title,
        location,
        city,
        state,
        company_revenue
    from (
        select * from fxf_prospects
        union all
        select * from pdl_prospects
    ) combined
)

select * from unified_prospects
order by prospect_id