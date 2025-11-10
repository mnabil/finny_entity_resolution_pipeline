
    
    

select
    company as unique_field,
    count(*) as n_records

from "finny_db"."public_staging_analysis"."company_analysis"
where company is not null
group by company
having count(*) > 1


