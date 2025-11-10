
    
    

select
    pdl_id as unique_field,
    count(*) as n_records

from "finny_db"."public_staging"."stg_pdl_data"
where pdl_id is not null
group by pdl_id
having count(*) > 1


