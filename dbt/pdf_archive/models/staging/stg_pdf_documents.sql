-- Staging: keep full text out of downstream table materializations (size).
{{ config(materialized="view") }}

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
    length(extracted_text) as extracted_text_char_length,
    coalesce(length(trim(extracted_text)), 0) > 0 as has_extracted_text
from {{ source("pipeline", "pdf_documents") }}
