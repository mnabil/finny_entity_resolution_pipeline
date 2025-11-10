{% macro create_merge_function() %}
  {{ return(adapter.dispatch('create_merge_function', 'dbt_service')()) }}
{% endmacro %}

{% macro postgres__create_merge_function() %}
  CREATE OR REPLACE FUNCTION merge_similar_entities()
  RETURNS VOID
  LANGUAGE plpgsql
  AS $$
  DECLARE
      rec RECORD;
      merge_count INTEGER := 0;
  BEGIN
      FOR rec IN
          SELECT canonical_id, merged_id
          FROM {{ ref('stg_entity_clusters') }}
      LOOP
          -- Lock both entities to prevent race conditions
          PERFORM * FROM {{ ref('stg_unified_prospects') }}
          WHERE prospect_id IN (rec.merged_id, rec.canonical_id) FOR UPDATE;

          -- Update canonical record fields only if null (fill in missing data)
          UPDATE {{ ref('stg_unified_prospects') }} p
          SET
              name = COALESCE(p.name, s.name),
              email = COALESCE(p.email, s.email),
              company = COALESCE(p.company, s.company),
              title = COALESCE(p.title, s.title),
              city = COALESCE(p.city, s.city),
              state = COALESCE(p.state, s.state),
              company_revenue = COALESCE(p.company_revenue, s.company_revenue)
          FROM {{ ref('stg_unified_prospects') }} s
          WHERE p.prospect_id = rec.canonical_id
            AND s.prospect_id = rec.merged_id;

          -- Mark merged record
          UPDATE {{ ref('stg_unified_prospects') }}
          SET status = 'merged'
          WHERE prospect_id = rec.merged_id;

          merge_count := merge_count + 1;
      END LOOP;

      RAISE NOTICE 'Merged % entities', merge_count;
  END;
  $$;
{% endmacro %}

{% macro default__create_merge_function() %}
  {{ exceptions.raise_compiler_error("create_merge_function macro only supports Postgres") }}
{% endmacro %}