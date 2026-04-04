-- Run manually if you already initialized Postgres with the older schema (host_accessible_path).
-- Example: docker compose exec -T postgres psql -U pdfuser -d pdfarchive -f - < postgres/migrations/001_local_pc_archive_path.sql
-- Or paste into your SQL client.

ALTER TABLE pdf_documents ADD COLUMN IF NOT EXISTS local_pc_archive_path TEXT;

UPDATE pdf_documents
SET local_pc_archive_path = COALESCE(
    NULLIF(TRIM(host_accessible_path), ''),
    'data/pdfs/' || stored_file_name
)
WHERE local_pc_archive_path IS NULL;

UPDATE pdf_documents
SET local_pc_archive_path = 'data/pdfs/' || stored_file_name
WHERE local_pc_archive_path IS NULL OR TRIM(local_pc_archive_path) = '';

ALTER TABLE pdf_documents ALTER COLUMN local_pc_archive_path SET NOT NULL;

ALTER TABLE pdf_documents DROP COLUMN IF EXISTS host_accessible_path;
