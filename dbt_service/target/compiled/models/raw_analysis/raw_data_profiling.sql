-- Raw data profiling analysis
-- This model analyzes data quality patterns in the raw data layer



with fxf_raw_profile as (
    select
        'fxf_data' as source_table,
        'raw' as data_layer,
        count(*) as total_records,
        count(distinct fxf_id) as unique_ids,
        count(*) - count(distinct fxf_id) as duplicate_ids,
        count(name) as non_null_names,
        count(*) - count(name) as null_names,
        count(email) as non_null_emails,
        count(*) - count(email) as null_emails,
        count(company) as non_null_companies,
        count(*) - count(company) as null_companies,
        count(company_revenue) as non_null_revenues,
        count(*) - count(company_revenue) as null_revenues,
        count(title) as non_null_titles,
        count(*) - count(title) as null_titles,
        count(location) as non_null_locations,
        count(*) - count(location) as null_locations,
        count(distinct company) as unique_companies,
        count(distinct location) as unique_locations,
        count(distinct title) as unique_titles
    from "finny_db"."public_raw"."raw_fxf_data"
),

pdl_raw_profile as (
    select
        'pdl_data' as source_table,
        'raw' as data_layer,
        count(*) as total_records,
        count(distinct pdl_id) as unique_ids,
        count(*) - count(distinct pdl_id) as duplicate_ids,
        count(name) as non_null_names,
        count(*) - count(name) as null_names,
        count(email) as non_null_emails,
        count(*) - count(email) as null_emails,
        count(company) as non_null_companies,
        count(*) - count(company) as null_companies,
        count(company_revenue) as non_null_revenues,
        count(*) - count(company_revenue) as null_revenues,
        count(title) as non_null_titles,
        count(*) - count(title) as null_titles,
        count(location) as non_null_locations,
        count(*) - count(location) as null_locations,
        count(distinct company) as unique_companies,
        count(distinct location) as unique_locations,
        count(distinct title) as unique_titles
    from "finny_db"."public_raw"."raw_pdl_data"
)

select * from fxf_raw_profile
union all
select * from pdl_raw_profile