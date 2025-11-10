
    
    

select
    pdl_id as unique_field,
    count(*) as n_records

from "finny_db"."public_raw"."raw_pdl_data"
where pdl_id is not null
group by pdl_id
having count(*) > 1


