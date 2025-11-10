-- Prospect matching ratio analysis
-- This model shows the distribution of unidentified vs matched prospects



with prospect_status_summary as (
    select
        status,
        count(*) as prospect_count,
        round(100.0 * count(*) / sum(count(*)) over(), 2) as percentage
    from "finny_db"."public_staging"."stg_unified_prospects"
    group by status
),

matching_summary as (
    select
        count(*) as total_matches,
        count(distinct source_id) as unique_sources_matched,
        count(distinct target_id) as unique_targets_matched
    from "finny_db"."public_staging"."stg_prospect_matches"
),

ratio_analysis as (
    select
        'Total Prospects' as metric_type,
        sum(prospect_count) as value,
        'prospects' as unit,
        null as percentage
    from prospect_status_summary
    
    union all
    
    select
        'Unidentified Prospects' as metric_type,
        prospect_count as value,
        'prospects' as unit,
        percentage
    from prospect_status_summary
    where status = 'unidentified'
    
    union all
    
    select
        'Merged Prospects' as metric_type,
        prospect_count as value,
        'prospects' as unit,
        percentage
    from prospect_status_summary
    where status = 'merged'
    
    union all
    
    select
        'Potential Matches Found' as metric_type,
        total_matches as value,
        'match pairs' as unit,
        null as percentage
    from matching_summary
    
    union all
    
    select
        'Unique Prospects with Matches' as metric_type,
        (unique_sources_matched + unique_targets_matched) as value,
        'prospects' as unit,
        round(100.0 * (unique_sources_matched + unique_targets_matched) / 
              (select sum(prospect_count) from prospect_status_summary), 2) as percentage
    from matching_summary
)

select 
    metric_type,
    value,
    unit,
    case 
        when percentage is not null then percentage || '%'
        else null
    end as percentage
from ratio_analysis
order by 
    case metric_type
        when 'Total Prospects' then 1
        when 'Unidentified Prospects' then 2  
        when 'Merged Prospects' then 3
        when 'Potential Matches Found' then 4
        when 'Unique Prospects with Matches' then 5
    end