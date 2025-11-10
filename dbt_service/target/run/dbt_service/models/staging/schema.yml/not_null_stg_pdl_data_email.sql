
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select email
from "finny_db"."public_staging"."stg_pdl_data"
where email is null



  
  
      
    ) dbt_internal_test