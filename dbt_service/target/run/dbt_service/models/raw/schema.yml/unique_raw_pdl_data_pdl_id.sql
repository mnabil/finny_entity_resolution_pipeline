
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    pdl_id as unique_field,
    count(*) as n_records

from "finny_db"."public_raw"."raw_pdl_data"
where pdl_id is not null
group by pdl_id
having count(*) > 1



  
  
      
    ) dbt_internal_test