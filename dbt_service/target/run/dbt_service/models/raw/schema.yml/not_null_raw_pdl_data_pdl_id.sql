
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pdl_id
from "finny_db"."public_raw"."raw_pdl_data"
where pdl_id is null



  
  
      
    ) dbt_internal_test