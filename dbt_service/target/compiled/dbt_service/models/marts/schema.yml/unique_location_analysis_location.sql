
    
    

select
    location as unique_field,
    count(*) as n_records

from "finny_db"."public_marts"."location_analysis"
where location is not null
group by location
having count(*) > 1


