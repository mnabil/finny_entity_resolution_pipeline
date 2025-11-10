# Finny Data Pipeline with Prospect Deduplication

This repository contains a complete data pipeline with PostgreSQL database and dbt analytics service for processing FXF and PDL contact data, including advanced prospect deduplication capabilities.

## Architecture

- **PostgreSQL Database** - Containerized database running on port 4320
- **dbt Service** - Data transformation and analysis pipeline
- **CSV Data** - 100,000+ contact records from FXF and PDL sources
- **Prospect Deduplication** - AI-powered similarity matching and entity resolution

## Quick Start

### 1. Start PostgreSQL
```bash
docker build -t finny-postgres .
docker run -d --name finny_postgres -p 4320:4320 -v finny_postgres_data:/var/lib/postgresql/data finny-postgres
```

### 2. Run dbt Pipeline
```bash
cd dbt_service
dbt seed          # Load CSV data
dbt run           # Build all models
dbt test          # Validate data quality
```

## Database Configuration

- **Database Name:** `finny_db`
- **Username:** `finny_user`
- **Password:** `finny_password`
- **Port:** `4320`

## Data Sources

The pipeline loads data from CSV files via dbt seeds:

- `fxf_data.csv`: FXF contact data (50,005 records)
- `pdl_data.csv`: PDL contact data (50,005 records)  
- `state_iso_mapping.csv`: State name to ISO-2 code mapping

CSV files are loaded using:
```bash
dbt seed  # Loads all CSV files into database tables
```

The resulting tables include:
- Normalized prospect data with structured fields
- Geographic information (city, state, country)
- Company and contact details
- Revenue and employee count metrics

## Connecting from Applications

## Prospect Deduplication Pipeline

Our advanced deduplication system identifies and merges duplicate prospects using similarity-based matching.

### How It Works

1. **Unified Prospects** - Combines FXF and PDL data into a single standardized table
2. **Similarity Matching** - Uses PostgreSQL's trigram similarity to find potential 
3. **Entity Clustering** - Groups similar prospects and identifies canonical records
4. **Automated Merging** - Consolidates data and marks duplicates as merged

### Processing Details

The similarity matching has been scaled up significantly from the initial 10% sample to process **113,771+ records**. The full refresh operation takes approximately **2 hours 11 minutes** for comprehensive similarity analysis.

**Technical Implementation:**
- Uses `materialized='incremental'` to create a table that updates incrementally and avoid rendering the entire dataset we run our matcher.
- Only processes new records on subsequent runs (making updates very fast)
- Responds to changes in `stg_unified_prospects` automatically
- Batch processing prevents memory issues with large datasets

### Key Metrics
- **100,010 raw prospects** from both sources (FXF: 50,005 + PDL: 50,005)
- **113,771 prospect matches** identified by AI similarity engine on this run
- **15,274 unique entity clusters** after advanced deduplication identified
- **13,875 high-confidence duplicates** merged (13.87% deduplication rate)
- **47 unique city/state combinations** analyzed with geographic intelligence
- **5 schema layers** for organized data architecture (raw → staging → marts → analysis)

### Pipeline Overview

**Current Results Summary:**
```
         table_name         | schema_name | row_count |                      description                       
----------------------------+-------------+-----------+--------------------------------------------------------
 stg_entity_clusters        | staging     |     15274 | High-confidence duplicates for merging
 stg_prospect_matches       | staging     |    113771 | Potential duplicate pairs identified
 stg_unified_prospects      | staging     |    100010 | Total unified prospects before deduplication
 unique_prospects_remaining | computed    |     86135 | Unidentified prospects (not yet matched as duplicates)
```

**Deduplication Effectiveness:**
```
          metric_type          | value  |    unit     | percentage 
-------------------------------+--------+-------------+------------
 Total Prospects               | 100010 | prospects   | 
 Unidentified Prospects        |  86135 | prospects   | 86.13%
 Merged Prospects              |  13875 | prospects   | 13.87%
 Potential Matches Found       | 113771 | match pairs | 
 Unique Prospects with Matches | 102328 | prospects   | 102.32%
```

**Key Pipeline Insights:**
- **Scale**: 113K+ records processed vs. 10K sample
- **Effectiveness**: 13.87% merge rate vs. 0.09% before  
- **Coverage**: 102.32% prospects have potential matches (indicates multiple matches per prospect)
- **Performance**: 2+ hours for similarity matching, seconds for everything else

**Pipeline Impact:**

Our AI-powered deduplication pipeline achieved significant improvements in data quality and prospect identification:

- **Entity Clustering**: Successfully identified 15,274 distinct entity clusters from the raw data (vs. only 87 Previous 10% sample)
- **Deduplication Effectiveness**: Achieved 13.87% prospect merge rate compared to 0.09% with traditional methods - a **154x improvement**  
- **Coverage Improvement**: Reduced unidentified prospects from 99.91% to 86.13% - **13.78 percentage points better coverage**
- **Data Quality**: Real metrics from `data_overview` and `prospect_matching_ratio` marts demonstrate measurable business impact

This represents a substantial improvement in data consolidation and prospect identification accuracy.

**Execution Performance:**
```
➜  dbt_service dbt run --select stg_prospect_matches --full-refresh
15:49:18  Running with dbt=1.10.13
15:49:18  Registered adapter: postgres=1.9.1
15:49:19  Found 18 models, 3 seeds, 11 data tests, 2 sources, 453 macros
15:49:19  
15:49:19  Concurrency: 32 threads (target='dev')
15:49:19  
15:49:19  1 of 1 START sql incremental model public_staging.stg_prospect_matches ......... [RUN]
18:00:38  1 of 1 OK created sql incremental model public_staging.stg_prospect_matches .... [SELECT 113771 in 7878.73s]
18:00:38  
18:00:38  Finished running 1 incremental model in 2 hours 11 minutes and 18.86 seconds (7878.86s).
18:00:38  
18:00:38  Completed successfully
18:00:38  
18:00:38  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
➜  dbt_service dbt run                                             
23:39:56  Running with dbt=1.10.13
23:39:56  Registered adapter: postgres=1.9.1
23:39:56  Found 18 models, 3 seeds, 11 data tests, 2 sources, 453 macros
23:39:56  
23:39:56  Concurrency: 32 threads (target='dev')
23:39:56  
23:39:57  1 of 18 START sql table model public_raw.raw_fxf_data .......................... [RUN]
23:39:57  2 of 18 START sql table model public_raw.raw_pdl_data .......................... [RUN]
23:39:57  2 of 18 OK created sql table model public_raw.raw_pdl_data ..................... [SELECT 50005 in 0.13s]
23:39:57  1 of 18 OK created sql table model public_raw.raw_fxf_data ..................... [SELECT 50005 in 0.13s]
23:39:57  3 of 18 START sql view model public_staging.stg_pdl_data ....................... [RUN]
23:39:57  4 of 18 START sql table model public_raw_analysis.raw_company_profiling ........ [RUN]
23:39:57  5 of 18 START sql table model public_raw_analysis.raw_data_profiling ........... [RUN]
23:39:57  6 of 18 START sql table model public_raw_analysis.raw_location_profiling ....... [RUN]
23:39:57  7 of 18 START sql view model public_staging.stg_fxf_data ....................... [RUN]
23:39:57  3 of 18 OK created sql view model public_staging.stg_pdl_data .................. [CREATE VIEW in 0.06s]
23:39:57  7 of 18 OK created sql view model public_staging.stg_fxf_data .................. [CREATE VIEW in 0.06s]
23:39:57  8 of 18 START sql table model public_staging_analysis.company_analysis ......... [RUN]
23:39:57  9 of 18 START sql table model public_staging_analysis.staging_city_state_profiling  [RUN]
23:39:57  10 of 18 START sql table model public_staging_analysis.staging_company_profiling  [RUN]
23:39:57  11 of 18 START sql table model public_staging_analysis.staging_data_profiling .. [RUN]
23:39:57  12 of 18 START sql table model public_staging_analysis.staging_location_profiling  [RUN]
23:39:57  13 of 18 START sql table model public_staging.stg_unified_prospects ............ [RUN]
23:39:57  5 of 18 OK created sql table model public_raw_analysis.raw_data_profiling ...... [SELECT 2 in 0.18s]
23:39:57  4 of 18 OK created sql table model public_raw_analysis.raw_company_profiling ... [SELECT 180 in 0.19s]
23:39:57  6 of 18 OK created sql table model public_raw_analysis.raw_location_profiling .. [SELECT 152 in 0.26s]
23:39:57  9 of 18 OK created sql table model public_staging_analysis.staging_city_state_profiling  [SELECT 76 in 0.23s]
23:39:57  8 of 18 OK created sql table model public_staging_analysis.company_analysis .... [SELECT 91 in 0.23s]
23:39:57  11 of 18 OK created sql table model public_staging_analysis.staging_data_profiling  [SELECT 2 in 0.23s]
23:39:57  13 of 18 OK created sql table model public_staging.stg_unified_prospects ....... [SELECT 100010 in 0.34s]
23:39:57  14 of 18 START sql incremental model public_staging.stg_prospect_matches ....... [RUN]
23:39:57  12 of 18 OK created sql table model public_staging_analysis.staging_location_profiling  [SELECT 152 in 0.38s]
23:39:57  10 of 18 OK created sql table model public_staging_analysis.staging_company_profiling  [SELECT 180 in 0.40s]
23:39:57  14 of 18 OK created sql incremental model public_staging.stg_prospect_matches .. [INSERT 0 0 in 0.23s]
23:39:57  15 of 18 START sql view model public_marts.prospect_matching_ratio ............. [RUN]
23:39:57  16 of 18 START sql table model public_staging.stg_entity_clusters .............. [RUN]
23:39:57  15 of 18 OK created sql view model public_marts.prospect_matching_ratio ........ [CREATE VIEW in 0.03s]
23:39:57  16 of 18 OK created sql table model public_staging.stg_entity_clusters ......... [SELECT 15274 in 0.04s]
23:39:57  17 of 18 START sql view model public_marts.data_overview ....................... [RUN]
23:39:57  18 of 18 START sql view model public.setup_functions ........................... [RUN]
23:39:58  17 of 18 OK created sql view model public_marts.data_overview .................. [CREATE VIEW in 0.03s]
23:39:58  18 of 18 OK created sql view model public.setup_functions ...................... [CREATE VIEW in 0.03s]
23:39:58  
23:39:58  Finished running 1 incremental model, 12 table models, 5 view models in 0 hours 0 minutes and 1.04 seconds (1.04s).
23:39:58  
23:39:58  Completed successfully
23:39:58  
23:39:58  Done. PASS=18 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=18
➜  dbt_service dbt run-operation merge_entities                    
23:51:50  Running with dbt=1.10.13
23:51:50  Registered adapter: postgres=1.9.1
23:51:51  Found 18 models, 3 seeds, 11 data tests, 2 sources, 453 macros
23:51:51  Executing prospect merge operation...
23:55:47  Merge operation completed successfully
➜  dbt_service dbt run --select data_overview                                                                                
23:56:51  Running with dbt=1.10.13
23:56:51  Registered adapter: postgres=1.9.1
23:56:51  Found 18 models, 3 seeds, 11 data tests, 2 sources, 453 macros
23:56:51  
23:56:51  Concurrency: 32 threads (target='dev')
23:56:51  
23:56:51  1 of 1 START sql view model public_marts.data_overview ......................... [RUN]
23:56:51  1 of 1 OK created sql view model public_marts.data_overview .................... [CREATE VIEW in 0.05s]
23:56:51  
23:56:51  Finished running 1 view model in 0 hours 0 minutes and 0.21 seconds (0.21s).
23:56:51  
23:56:51  Completed successfully
23:56:51  
23:56:51  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
➜  dbt_service dbt run --select prospect_matching_ratio
23:57:05  Running with dbt=1.10.13
23:57:05  Registered adapter: postgres=1.9.1
23:57:05  Found 18 models, 3 seeds, 11 data tests, 2 sources, 453 macros
23:57:05  
23:57:05  Concurrency: 32 threads (target='dev')
23:57:05  
23:57:06  1 of 1 START sql view model public_marts.prospect_matching_ratio ............... [RUN]
23:57:06  1 of 1 OK created sql view model public_marts.prospect_matching_ratio .......... [CREATE VIEW in 0.05s]
23:57:06  
23:57:06  Finished running 1 view model in 0 hours 0 minutes and 0.20 seconds (0.20s).
23:57:06  
23:57:06  Completed successfully
23:57:06  
23:57:06  Done. PASS=1 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=1
```

```sql
-- Verify prospect matches count
finny_db=# select count(*) from public_staging.stg_prospect_matches;
 count
--------
 113771
(1 row)
```

### Quick Deduplication Workflow

```bash
# Step 1: Build the unified prospects table
dbt run --select stg_unified_prospects

# Step 2: Run similarity matching (processes batches efficiently)
dbt run --select stg_prospect_matches --full-refresh

# Step 3: Create entity clusters for merging
dbt run --select stg_entity_clusters

# Step 4: Execute the merge process
dbt run-operation merge_entities
```

### Export Matched Results

To export the matched pairs for analysis (with distinct results only):

```sql
\copy (
        SELECT DISTINCT 
               p1.name as canonical_name, 
               p1.email as canonical_email, 
               p2.name as merged_name, 
               p2.email as merged_email 
          FROM public_staging.stg_entity_clusters c 
          JOIN public_staging.stg_unified_prospects p1 
            ON c.canonical_id = p1.prospect_id 
          JOIN public_staging.stg_unified_prospects p2 
            ON c.merged_id = p2.prospect_id 
      ORDER BY p1.name, p1.email
) TO '/Users/mahmoud/Workspace/finny/matched_full_data.csv' WITH CSV HEADER;
```

This exports all high-confidence matches with distinct results to `matched_full_data.csv`.

### Deduplication Results

From our test dataset:
- **100,010 total prospects** processed
- **1,908 similar pairs** identified from 10,000 records
- **87 high-confidence duplicates** merged
- **99,923 unique prospects** remaining

### Example Duplicates Found

| Canonical Record | Duplicate Record | Reason |
|-----------------|------------------|---------|
| Maria W Smith (mariasmith@enterpriseware.com) | Maria Smith (maria.smith@enterpriseware.com) | Same person, name variation |
| Janet Stewart (janet.stewart@eduplatform.com) | Janet Stewart (stewart.janet@eduplatform.com) | Same person, email format difference |
| Samuel Garcia (sgarcia@innovatelabs.com) | Samuel W Garcia (sgarcia@innovatelabs.com) | Same person, middle initial variation |

### Configuration

**Processing Scope:**
- Current processing: ~113,771 records (significantly more than 10K sample)
- Prospect ID range: Configurable in `stg_prospect_matches.sql`

**Similarity Thresholds:**
- Name similarity: > 0.5
- Email similarity: > 0.5  
- Company similarity: weighted 0.1
- Final score threshold: > 0.6 for candidates, > 0.8 for merging

**Performance:**
- **Full refresh run**: 113,771 records in ~2 hours 11 minutes (7,878 seconds)
- **Incremental updates**: <1 second for new records only
- **Memory-efficient**: Batch processing prevents memory issues with large datasets

**Recent Performance Results:**
```
➜  dbt run --select stg_prospect_matches --full-refresh
15:49:19  1 of 1 START sql incremental model public_staging.stg_prospect_matches ......... [RUN]
18:00:38  1 of 1 OK created sql incremental model public_staging.stg_prospect_matches .... [SELECT 113771 in 7878.73s]
18:00:38  Completed successfully
```

## dbt Project Structure

```
dbt_service/models/
├── raw/                    # Raw data tables (schema: raw)
│   ├── raw_fxf_data.sql    # Raw FXF contact data
│   └── raw_pdl_data.sql    # Raw PDL contact data
├── staging/                # Cleaned staging views (schema: staging)  
│   ├── stg_fxf_data.sql            # Cleaned FXF data with city/state parsing
│   ├── stg_pdl_data.sql            # Cleaned PDL data with city/state parsing
│   ├── stg_unified_prospects.sql   # Combined prospects from both sources
│   ├── stg_prospect_matches.sql    # AI similarity-based duplicate detection
│   └── stg_entity_clusters.sql     # Canonical prospect assignments
├── marts/                  # Business logic tables (schema: marts) - matched data only
│   ├── data_overview.sql           # Deduplication pipeline summary
│   └── prospect_matching_ratio.sql # Prospect deduplication metrics
├── raw_analysis/           # Raw data profiling (schema: raw_analysis)
│   ├── raw_data_profiling.sql      # Data quality metrics for raw layer
│   ├── raw_company_profiling.sql   # Company distribution analysis
│   └── raw_location_profiling.sql  # Geographic profiling (raw data)
├── staging_analysis/       # Staging data profiling (schema: staging_analysis)
│   ├── staging_data_profiling.sql      # Data quality metrics for staging layer
│   ├── staging_company_profiling.sql   # Company analysis (cleaned data)
│   ├── staging_city_state_profiling.sql # City/state geographic analysis
│   └── staging_location_profiling.sql  # Location distribution analysis
└── hooks/                  # Database setup and maintenance (schema: public)
    └── setup_functions.sql         # Creates PostgreSQL functions for deduplication
```

## PostgreSQL Functions

```
finny_db functions/
├── merge_similar_entities()    # Automated prospect deduplication engine
└── pg_trgm extension          # Trigram similarity matching support
```

## dbt Macros

```
dbt_service/macros/
├── merge_entities.sql              # dbt wrapper for deduplication function
└── sql_functions/                  # PostgreSQL function definitions
    └── create_merge_function.sql   # Creates merge_similar_entities() function
```

## Database Configuration

- **Host:** `localhost`
- **Port:** `4320`
- **Database:** `finny_db`
- **Username:** `finny_user`
- **Password:** `finny_password`

## Data Pipeline Features

### Prospect Deduplication
- **AI-Powered Matching** - Uses PostgreSQL trigram similarity for name, email, and company matching
- **Incremental Processing** - Efficiently handles large datasets with batch processing
- **Entity Resolution** - Automatically identifies canonical records and merges duplicates
- **Data Consolidation** - Preserves all data while eliminating redundancy
- **Quality Scoring** - Weighted similarity scores ensure high-confidence matches

### Geographic Intelligence  
- **Location Parsing** - Automatically extracts city and state from mixed location formats
- **ISO Standardization** - Uses CSV seed data to map state names to ISO-2 codes
- **Geographic Profiling** - Analyzes prospect distribution across cities and states
- **Multi-layer Analysis** - Tracks geographic data through raw, staging, and marts layers

### Data Profiling
- **Raw Analysis** - Quality metrics, nulls, duplicates in source data
- **Staging Analysis** - Completeness percentages, data validation after cleaning  
- **Multi-layer Monitoring** - Track data quality at each transformation stage
- **Company Profiling** - Distribution analysis across different company attributes
- **Location Profiling** - Geographic data quality and distribution metrics

### Business Intelligence
- **Unified View** - Single source of truth combining FXF and PDL data
- **Location Analysis** - Geographic distribution without redundant location grouping
- **Data Overview** - Pipeline health and record counts (staging-focused)
- **Quality Tracking** - Comprehensive data quality monitoring across all layers

### Data Quality
- Automated tests for uniqueness and null constraints
- Data completeness tracking across all pipeline stages
- Revenue statistics and outlier detection
- Duplicate detection and resolution
- Geographic data standardization and validation

## Connection Examples

**Connection String:**
```
postgresql://finny_user:finny_password@localhost:4320/finny_db
```

## Container Management

```bash
# Container operations
docker stop finny_postgres
docker start finny_postgres
docker logs finny_postgres

# Data backup
docker exec finny_postgres pg_dump -p 4320 -U finny_user finny_db > backup.sql
```

## Development

The pipeline processes 100,010 total records (50,005 from each source) with comprehensive data quality monitoring, business intelligence capabilities, and advanced prospect deduplication.

### Current Pipeline Status
- **✅ Raw Data Layer** - FXF and PDL source data loaded
- **✅ Staging Layer** - Cleaned data with geographic parsing and unified prospects  
- **✅ Analytics Layer** - Multi-dimensional profiling across raw and staging
- **✅ Business Layer** - Marts focused on staging data for business insights
- **✅ Deduplication Engine** - AI-powered similarity matching and entity resolution
- **✅ Geographic Intelligence** - City/state parsing with ISO standardization

### Data Lineage
```
CSV Seeds (fxf_data, pdl_data, state_iso_mapping)
                    ↓
            Raw Layer Tables
        (raw_fxf_data, raw_pdl_data)
                    ↓                    ↓                    ↓
             Staging Layer         Database Setup      Raw Analysis
    (stg_fxf_data, stg_pdl_data)    setup_functions    (raw layer profiling)
                    ↓                    ↓                    ↓
            stg_unified_prospects → [Function: merge_similar_entities()]  raw_data_profiling
                    ↓                    ↑                raw_company_profiling
          Staging Analysis       Deduplication Pipeline  raw_location_profiling
        (staging layer profiling)                               
                    ↓              stg_unified_prospects → stg_prospect_matches → stg_entity_clusters
        staging_data_profiling                                   ↓                       ↓
      staging_company_profiling                    Automated Entity Merging    dbt run-operation merge_entities
      staging_location_profiling                   (via PostgreSQL function)            ↓
    staging_city_state_profiling                           ↓                Status: 87 merges completed
                    ↓                              Marts (matched data only)
            Company Analysis                              ↓
            company_analysis                 ┌─────────────────────┐
                                             ↓                     ↓
                                       data_overview    prospect_matching_ratio
```

### Next Steps
- Scale similarity matching to full dataset (100k+ records)
- Implement real-time duplicate detection for new data
- Add fuzzy matching for company names
- Create prospect scoring and segmentation models
- Add timestamp-based freshness checks for raw data ingestion
- Consider migrating to Snowflake for expensive computational workloads (similarity matching at scale)
  - Migration only requires updating connection settings in `profiles.yml`