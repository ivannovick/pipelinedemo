-- Application data: PDFs land here only after the batch consumer reads from RabbitMQ Streams.
--
-- local_pc_archive_path: where the archived PDF lives on your machine.
--   - If you set PDF_HOST_BASE_DIR in .env to the absolute path of ./data/pdfs, this column stores that full path + filename.
--   - Otherwise it stores a repo-relative path: data/pdfs/<stored_file_name> (same files as the Compose bind mount).

CREATE TABLE IF NOT EXISTS pdf_documents (
    id BIGSERIAL PRIMARY KEY,
    source_file_url TEXT,
    original_file_name TEXT NOT NULL DEFAULT 'document.pdf',
    stored_file_name TEXT NOT NULL,
    container_path TEXT NOT NULL,
    local_pc_archive_path TEXT NOT NULL,
    extracted_text TEXT NOT NULL,
    source_stream TEXT NOT NULL,
    stream_offset BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pdf_documents_stored_file_name_key UNIQUE (stored_file_name),
    CONSTRAINT pdf_documents_stream_offset_key UNIQUE (source_stream, stream_offset)
);

CREATE INDEX IF NOT EXISTS idx_pdf_documents_created_at ON pdf_documents (created_at);
