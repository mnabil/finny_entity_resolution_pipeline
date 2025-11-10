
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company
from "finny_db"."public_staging_analysis"."company_analysis"
where company is null



  
  
      
    ) dbt_internal_test