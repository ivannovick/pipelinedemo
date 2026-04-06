-- Narrow FTS helper: id + precomputed tsvector (GIN index). Join public.pdf_documents on id for filenames/snippets.
-- Example (after dbt run): select d.original_file_name, ts_rank(f.search_vector, q) as rank
--   from dbt_analytics.pdf_documents_fts f
--   join public.pdf_documents d on d.id = f.id,
--   websearch_to_tsquery('english', 'your terms') q
--   where f.search_vector @@ q order by rank desc;
{{
    config(
        materialized="table",
        post_hook=[
            "create index if not exists pdf_documents_fts_gin on {{ this }} using gin (search_vector)",
        ],
    )
}}

select
    id,
    to_tsvector('english', coalesce(extracted_text, '')) as search_vector
from {{ source("pipeline", "pdf_documents") }}
