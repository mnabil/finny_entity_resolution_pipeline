-- Prospect similarity matching for deduplication
-- This model identifies potential duplicate prospects based on name, email, and company similarity



WITH base AS (
  SELECT *
  FROM "finny_db"."public_staging"."stg_unified_prospects"
  WHERE status IS DISTINCT FROM 'merged'
    AND prospect_id <= 2000000  -- Increase to 10000 records
  
    -- Only process new prospects in incremental runs
    AND prospect_id > (SELECT COALESCE(MAX(source_id), 0) FROM "finny_db"."public_staging"."stg_prospect_matches")
  
),

pairs AS (
  SELECT 
    a.prospect_id AS source_id,
    b.prospect_id AS target_id,
    similarity(a.name, b.name) AS name_sim,
    similarity(a.email, b.email) AS email_sim,
    similarity(a.company, b.company) AS company_sim,
    (
      0.5 * similarity(a.name, b.name) +
      0.4 * similarity(a.email, b.email) +
      0.1 * similarity(a.company, b.company)
    ) AS total_score
  FROM base a
  JOIN base b
    ON a.prospect_id < b.prospect_id
  WHERE (
    similarity(a.name, b.name) > 0.5 and  -- Lower threshold
    similarity(a.email, b.email) > 0.4   -- Lower threshold
  )
)

SELECT 
  source_id,
  target_id,
  name_sim,
  email_sim,
  company_sim,
  total_score
FROM pairs
WHERE total_score > 0.6  -- Lower threshold for final results