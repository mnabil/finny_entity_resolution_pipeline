
  
    

  create  table "finny_db"."public_marts"."location_analysis__dbt_tmp"
  
  
    as
  
  (
    -- Location analysis across both data sources with structured city and state data



with all_contacts as (
    select
        'fxf' as data_source,
        name,
        email,
        company,
        title,
        city,
        state,
        company_revenue
    from "finny_db"."public_staging"."stg_fxf_data"
    
    union all
    
    select
        'pdl' as data_source,
        name,
        email,
        company,
        title,
        city,
        state,
        company_revenue
    from "finny_db"."public_staging"."stg_pdl_data"
),

city_state_stats as (
    select
        city,
        state,
        count(*) as contact_count,
        count(distinct company) as company_count,
        count(distinct title) as unique_titles,
        array_agg(distinct data_source) as data_sources,
        avg(company_revenue) as avg_revenue
    from all_contacts
    where city is not null and state is not null
    group by city, state
),

state_summary as (
    select
        state,
        count(distinct city) as cities_in_state,
        sum(contact_count) as total_contacts_in_state,
        sum(company_count) as total_companies_in_state,
        avg(avg_revenue) as avg_state_revenue
    from city_state_stats
    where state is not null
    group by state
)

select
    cs.city,
    cs.state,
    cs.contact_count,
    cs.company_count,
    cs.unique_titles,
    cs.data_sources,
    round(cs.avg_revenue::numeric, 2) as avg_revenue,
    -- Add state-level context
    ss.total_contacts_in_state,
    ss.cities_in_state,
    round(100.0 * cs.contact_count / ss.total_contacts_in_state, 2) as pct_of_state_contacts
from city_state_stats cs
left join state_summary ss on cs.state = ss.state
order by cs.contact_count desc
  );
  