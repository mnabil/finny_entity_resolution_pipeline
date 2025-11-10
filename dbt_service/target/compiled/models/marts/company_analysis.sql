-- Combined company analysis from both FXF and PDL data sources



with fxf_companies as (
    select
        'fxf' as data_source,
        company,
        count(*) as employee_count,
        count(distinct title) as unique_titles,
        avg(company_revenue) as avg_revenue,
        array_agg(distinct location) as locations
    from "finny_db"."public_staging"."stg_fxf_data"
    where company is not null
    group by company
),

pdl_companies as (
    select
        'pdl' as data_source,
        company,
        count(*) as employee_count,
        count(distinct title) as unique_titles,
        avg(company_revenue) as avg_revenue,
        array_agg(distinct location) as locations
    from "finny_db"."public_staging"."stg_pdl_data"
    where company is not null
    group by company
),

combined_companies as (
    select * from fxf_companies
    union all
    select * from pdl_companies
)

select
    company,
    array_agg(data_source) as data_sources,
    sum(employee_count) as total_employees,
    sum(unique_titles) as total_unique_titles,
    avg(avg_revenue) as average_revenue,
    array_agg(distinct location_item) as all_locations
from combined_companies,
     unnest(locations) as location_item
group by company
order by total_employees desc