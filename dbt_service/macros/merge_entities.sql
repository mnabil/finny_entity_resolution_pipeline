{% macro merge_entities() %}
  {% if execute %}
    {% set query %}
      SELECT merge_similar_entities();
    {% endset %}
    
    {% do log("Executing prospect merge operation...", info=True) %}
    {% set results = run_query(query) %}
    {% do log("Merge operation completed successfully", info=True) %}
  {% endif %}
{% endmacro %}