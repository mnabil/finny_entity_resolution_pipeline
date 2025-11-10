
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location
from "finny_db"."public_marts"."location_analysis"
where location is null



  
  
      
    ) dbt_internal_test