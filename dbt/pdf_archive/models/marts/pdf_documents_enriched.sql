-- Post-processed facts for reporting / BI (no full extracted_text column).
{{ config(materialized="table") }}

select
    id,
    source_file_url,
    original_file_name,
    stored_file_name,
    container_path,
    local_pc_archive_path,
    source_stream,
    stream_offset,
    created_at,
    extracted_text_char_length,
    has_extracted_text,
    (created_at at time zone 'UTC')::date as ingest_date_utc,
    lower(split_part(original_file_name, '.', 1)) as file_base_name,
    round(extracted_text_char_length::numeric / 5.0, 0)::bigint as approx_word_count
from {{ ref("stg_pdf_documents") }}
