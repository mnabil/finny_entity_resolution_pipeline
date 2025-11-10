
  create view "finny_db"."public_marts"."data_overview__dbt_tmp"
    
    
  as (
    -- Data overview from deduplication pipeline
-- This model provides a summary of our prospect matching and deduplication results



select
    'stg_unified_prospects' as table_name,
    'staging' as schema_name,
    count(*) as row_count,
    'Total unified prospects before deduplication' as description
from "finny_db"."public_staging"."stg_unified_prospects"

union all

select
    'stg_prospect_matches' as table_name,
    'staging' as schema_name,
    count(*) as row_count,
    'Potential duplicate pairs identified' as description
from "finny_db"."public_staging"."stg_prospect_matches"

union all

select
    'stg_entity_clusters' as table_name,
    'staging' as schema_name,
    count(*) as row_count,
    'High-confidence duplicates for merging' as description
from "finny_db"."public_staging"."stg_entity_clusters"

union all

select
    'unique_prospects_remaining' as table_name,
    'computed' as schema_name,
    count(*) as row_count,
    'Unidentified prospects (not yet matched as duplicates)' as description
from "finny_db"."public_staging"."stg_unified_prospects"
where status is distinct from 'merged'
  );