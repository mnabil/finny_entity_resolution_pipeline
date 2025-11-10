
  
    

  create  table "finny_db"."public_raw"."raw_pdl_data__dbt_tmp"
  
  
    as
  
  (
    -- Raw PDL data from seed  
-- This model creates a table in the raw schema from our CSV data



select * from "finny_db"."public"."pdl_data"
  );
  