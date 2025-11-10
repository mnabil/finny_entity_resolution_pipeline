
  
    

  create  table "finny_db"."public_staging_analysis"."company_analysis__dbt_tmp"
  
  
    as
  
  (
    -- Combined company analysis from both FXF and PDL data sources with structured location data



with fxf_companies as (
    select
        'fxf' as data_source,
        company,
        count(*) as employee_count,
        count(distinct title) as unique_titles,
        avg(company_revenue) as avg_revenue,
        array_agg(distinct location) as locations,
        array_agg(distinct city) filter (where city is not null) as cities,
        array_agg(distinct state) filter (where state is not null) as states,
        count(distinct city) as unique_cities,
        count(distinct state) as unique_states
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
        array_agg(distinct location) as locations,
        array_agg(distinct city) filter (where city is not null) as cities,
        array_agg(distinct state) filter (where state is not null) as states,
        count(distinct city) as unique_cities,
        count(distinct state) as unique_states
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
    array_agg(distinct data_source) as data_sources,
    sum(employee_count) as total_employees,
    sum(unique_titles) as total_unique_titles,
    avg(avg_revenue) as average_revenue,
    sum(unique_cities) as total_unique_cities,
    sum(unique_states) as total_unique_states,
    array_agg(distinct location_item) filter (where location_item is not null) as all_locations,
    array_agg(distinct city_item) filter (where city_item is not null) as all_cities,
    array_agg(distinct state_item) filter (where state_item is not null) as all_states
from (
    select 
        company, data_source, employee_count, unique_titles, avg_revenue, unique_cities, unique_states,
        unnest(locations) as location_item,
        unnest(cities) as city_item,
        unnest(states) as state_item
    from combined_companies
) expanded
group by company
order by total_employees desc
  );
  