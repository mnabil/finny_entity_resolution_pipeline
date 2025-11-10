
  create view "finny_db"."public"."setup_functions__dbt_tmp"
    
    
  as (
    -- Database function setup for deduplication pipeline
-- This model ensures the merge_similar_entities() function exists in the database



select 'Function merge_similar_entities() created' as status
  );