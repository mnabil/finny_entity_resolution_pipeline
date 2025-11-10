
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fxf_id
from "finny_db"."public_staging"."stg_fxf_data"
where fxf_id is null



  
  
      
    ) dbt_internal_test