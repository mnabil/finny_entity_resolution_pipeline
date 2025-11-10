
    
    

select
    fxf_id as unique_field,
    count(*) as n_records

from "finny_db"."public_staging"."stg_fxf_data"
where fxf_id is not null
group by fxf_id
having count(*) > 1


