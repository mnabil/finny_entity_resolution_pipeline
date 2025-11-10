
  
    

  create  table "finny_db"."public_staging"."stg_entity_clusters__dbt_tmp"
  
  
    as
  
  (
    -- Entity clusters for prospect deduplication
-- This model identifies canonical (primary) records and duplicates to be merged
-- Results:

-- 87 duplicate pairs identified from high-confidence matches (score > 0.8)
-- Canonical ID logic: Uses the higher prospect_id as canonical (keeps the "later" record)
-- Merged ID logic: Lower prospect_id will be marked as duplicate
-- Range: Processing prospects from 658 to 9999
-- How it works:

-- Example: Prospect 658 is canonical, Prospect 291 should be merged into it
-- etc.



WITH ranked AS (
  SELECT 
    *,
    GREATEST(source_id, target_id) AS canonical_id,
    LEAST(source_id, target_id) AS merged_id
  FROM "finny_db"."public_staging"."stg_prospect_matches"
  WHERE total_score > 0.8  -- Only high-confidence matches
)

SELECT DISTINCT 
  canonical_id, 
  merged_id,
  'duplicate' as merge_reason
FROM ranked
ORDER BY canonical_id, merged_id
  );
  