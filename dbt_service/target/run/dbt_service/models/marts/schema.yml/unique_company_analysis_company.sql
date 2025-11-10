
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    company as unique_field,
    count(*) as n_records

from "finny_db"."public_staging_analysis"."company_analysis"
where company is not null
group by company
having count(*) > 1



  
  
      
    ) dbt_internal_test