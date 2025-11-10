
      
        delete from "finny_db"."public_staging"."stg_prospect_matches" as DBT_INTERNAL_DEST
        where (source_id, target_id) in (
            select distinct source_id, target_id
            from "stg_prospect_matches__dbt_tmp183957732187" as DBT_INTERNAL_SOURCE
        );

    

    insert into "finny_db"."public_staging"."stg_prospect_matches" ("source_id", "target_id", "name_sim", "email_sim", "company_sim", "total_score")
    (
        select "source_id", "target_id", "name_sim", "email_sim", "company_sim", "total_score"
        from "stg_prospect_matches__dbt_tmp183957732187"
    )
  