-- Database function setup for deduplication pipeline
-- This model ensures the merge_similar_entities() function exists in the database

{{ config(
    materialized='view',
    post_hook="{{ create_merge_function() }}"
) }}

select 'Function merge_similar_entities() created' as status