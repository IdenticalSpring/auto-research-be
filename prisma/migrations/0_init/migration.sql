-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateEnum
CREATE TYPE "user_role" AS ENUM ('user', 'admin');

-- CreateEnum
CREATE TYPE "job_status" AS ENUM ('pending', 'processing', 'finished', 'error', 'suspended');

-- CreateEnum
CREATE TYPE "module_type" AS ENUM ('document_setup', 'write_report', 'chatbot', 'enhancement');

-- CreateEnum
CREATE TYPE "doc_status" AS ENUM ('draft', 'processing', 'completed');

-- CreateEnum
CREATE TYPE "reference_download_status" AS ENUM ('pending', 'downloaded', 'failed');

-- CreateEnum
CREATE TYPE "file_processing_status" AS ENUM ('uploaded', 'processing', 'embedded', 'error');

-- CreateEnum
CREATE TYPE "analysis_mode" AS ENUM ('Auto', 'Interactive', 'Manual');

-- CreateEnum
CREATE TYPE "variable_type" AS ENUM ('Numeric', 'Categorical', 'String', 'Boolean', 'DateTime');

-- CreateEnum
CREATE TYPE "variable_role" AS ENUM ('Dependent', 'Independent', 'Control', 'Moderator', 'Mediator');

-- CreateEnum
CREATE TYPE "slide_status" AS ENUM ('generating', 'completed', 'error');

-- CreateEnum
CREATE TYPE "chat_mode" AS ENUM ('outer_chatbot', 'inner_chatbot', 'writer_chatbot');

-- CreateEnum
CREATE TYPE "chat_sender" AS ENUM ('user', 'assistant');

-- CreateEnum
CREATE TYPE "enhancement_type" AS ENUM ('rewrite', 'improve_clarity', 'fix_grammar', 'make_academic', 'shorten', 'expand', 'translate');

-- CreateEnum
CREATE TYPE "suggestion_status" AS ENUM ('pending', 'applied', 'dismissed');

-- CreateTable
CREATE TABLE "users" (
    "id" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "full_name" TEXT,
    "user_name" TEXT NOT NULL,
    "password" TEXT NOT NULL,
    "role" "user_role" NOT NULL DEFAULT 'user',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "users_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "documents" (
    "id" TEXT NOT NULL,
    "user_id" TEXT NOT NULL,
    "title" TEXT,
    "field" TEXT,
    "document_type" TEXT,
    "language" TEXT,
    "domains" JSONB,
    "subdomains" JSONB,
    "number_of_domains" INTEGER,
    "number_of_subdomains" INTEGER,
    "web_search_enabled" BOOLEAN,
    "research_type" TEXT,
    "status" "doc_status" NOT NULL DEFAULT 'draft',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "documents_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "proposals" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "problem_statement" TEXT,
    "motivation" TEXT,
    "research_gap" TEXT,
    "final_model" JSONB,
    "hypothesis" JSONB,
    "variables" JSONB,
    "survey_questions" JSONB,
    "questions" JSONB,
    "is_selected" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "proposals_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "document_keywords" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "keywords_main" JSONB,
    "keywords_sub" JSONB,
    "search_query" TEXT,
    "country" TEXT,
    "year_from" INTEGER,
    "year_to" INTEGER,
    "only_open_access" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "document_keywords_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "references" (
    "id" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "authors" TEXT,
    "journal" TEXT,
    "year" INTEGER,
    "open_access" BOOLEAN NOT NULL DEFAULT false,
    "downloaded_status" "reference_download_status" NOT NULL DEFAULT 'pending',
    "url" TEXT,
    "abstract" TEXT,
    "q" TEXT,
    "citation" INTEGER DEFAULT 0,
    "user_language_title" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "references_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "document_references" (
    "document_id" TEXT NOT NULL,
    "reference_id" TEXT NOT NULL,

    CONSTRAINT "document_references_pkey" PRIMARY KEY ("document_id","reference_id")
);

-- CreateTable
CREATE TABLE "user_uploaded_documents" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "file_name" TEXT NOT NULL,
    "title" TEXT,
    "processing_status" "file_processing_status" NOT NULL DEFAULT 'uploaded',
    "is_embedded" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "user_uploaded_documents_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "outlines" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "total_word_count" INTEGER NOT NULL DEFAULT 0,
    "structure" JSONB NOT NULL,
    "outline_code" JSONB,
    "word_count_str" TEXT,
    "headings" JSONB,
    "headings_percent" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "outlines_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "reports" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "content_markdown" TEXT,
    "references_style" TEXT,
    "target_language" TEXT,
    "version" INTEGER NOT NULL DEFAULT 1,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "reports_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "datasets" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "file_name" TEXT,
    "file_path" TEXT,
    "raw_data" JSONB,
    "analysis_mode" "analysis_mode" NOT NULL DEFAULT 'Auto',
    "feedback" TEXT,
    "analysis_results" JSONB,
    "analyze_log" TEXT,
    "detailed_logs" JSONB,
    "generated_files" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "datasets_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "dataset_variables" (
    "id" TEXT NOT NULL,
    "dataset_id" TEXT NOT NULL,
    "variable_name" TEXT NOT NULL,
    "variable_code" TEXT,
    "variable_type" "variable_type",
    "variable_role" "variable_role",
    "scale" TEXT,
    "unit" TEXT,
    "collection_method" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "dataset_variables_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "proposed_methods" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "user_requirements" TEXT,
    "research_model" JSONB,
    "hypotheses" JSONB,
    "variables" JSONB,
    "survey_questions" JSONB,
    "questions" JSONB,
    "is_accepted" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "proposed_methods_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "generated_slides" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "slide_style" TEXT,
    "manual_mode" BOOLEAN NOT NULL DEFAULT false,
    "language" TEXT,
    "pptx_url" TEXT,
    "slides_json" JSONB,
    "status" "slide_status" NOT NULL DEFAULT 'generating',
    "progress_step" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "generated_slides_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "chat_sessions" (
    "id" TEXT NOT NULL,
    "user_id" TEXT NOT NULL,
    "document_id" TEXT,
    "mode" "chat_mode" NOT NULL,
    "title" TEXT NOT NULL DEFAULT 'New Chat',
    "language" TEXT,
    "model" TEXT,
    "web_search_enabled" BOOLEAN,
    "conv_id" TEXT,
    "thread_id" TEXT,
    "template_key" TEXT,
    "summary" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "chat_sessions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "chat_messages" (
    "id" TEXT NOT NULL,
    "session_id" TEXT NOT NULL,
    "sender" "chat_sender" NOT NULL,
    "message" TEXT NOT NULL,
    "metadata" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "chat_messages_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "enhancement_sessions" (
    "id" TEXT NOT NULL,
    "document_id" TEXT NOT NULL,
    "selected_text" TEXT NOT NULL,
    "context_text" TEXT,
    "user_prompt" TEXT,
    "enhancement_type" "enhancement_type",
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "enhancement_sessions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "enhancement_suggestions" (
    "id" TEXT NOT NULL,
    "session_id" TEXT NOT NULL,
    "suggested_text" TEXT NOT NULL,
    "explanation" TEXT,
    "status" "suggestion_status" NOT NULL DEFAULT 'pending',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "enhancement_suggestions_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "jobs" (
    "id" TEXT NOT NULL,
    "module" "module_type" NOT NULL,
    "action" TEXT NOT NULL,
    "document_id" TEXT,
    "session_id" TEXT,
    "thread_id" TEXT,
    "conv_id" TEXT,
    "status" "job_status" NOT NULL DEFAULT 'pending',
    "resume_step" TEXT,
    "payload" JSONB,
    "result" JSONB,
    "error_code" TEXT,
    "error_message" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "jobs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "token_usages" (
    "id" TEXT NOT NULL,
    "user_id" TEXT NOT NULL,
    "document_id" TEXT,
    "conv_id" TEXT,
    "session_id" TEXT,
    "event_type" TEXT NOT NULL,
    "model_id" TEXT,
    "input_tokens" INTEGER NOT NULL DEFAULT 0,
    "output_tokens" INTEGER NOT NULL DEFAULT 0,
    "embed_tokens" INTEGER NOT NULL DEFAULT 0,
    "embed_model" TEXT,
    "web_search_call" INTEGER NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "token_usages_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "users_email_key" ON "users"("email");

-- CreateIndex
CREATE UNIQUE INDEX "users_user_name_key" ON "users"("user_name");

-- CreateIndex
CREATE UNIQUE INDEX "outlines_document_id_key" ON "outlines"("document_id");

-- CreateIndex
CREATE UNIQUE INDEX "reports_document_id_key" ON "reports"("document_id");

-- CreateIndex
CREATE UNIQUE INDEX "chat_sessions_conv_id_key" ON "chat_sessions"("conv_id");

-- CreateIndex
CREATE INDEX "token_usages_user_id_created_at_idx" ON "token_usages"("user_id", "created_at");

-- CreateIndex
CREATE INDEX "token_usages_document_id_idx" ON "token_usages"("document_id");

-- AddForeignKey
ALTER TABLE "documents" ADD CONSTRAINT "documents_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "proposals" ADD CONSTRAINT "proposals_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "document_keywords" ADD CONSTRAINT "document_keywords_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "document_references" ADD CONSTRAINT "document_references_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "document_references" ADD CONSTRAINT "document_references_reference_id_fkey" FOREIGN KEY ("reference_id") REFERENCES "references"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "user_uploaded_documents" ADD CONSTRAINT "user_uploaded_documents_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "outlines" ADD CONSTRAINT "outlines_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "reports" ADD CONSTRAINT "reports_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "datasets" ADD CONSTRAINT "datasets_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "dataset_variables" ADD CONSTRAINT "dataset_variables_dataset_id_fkey" FOREIGN KEY ("dataset_id") REFERENCES "datasets"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "proposed_methods" ADD CONSTRAINT "proposed_methods_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "generated_slides" ADD CONSTRAINT "generated_slides_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "chat_sessions" ADD CONSTRAINT "chat_sessions_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "chat_sessions" ADD CONSTRAINT "chat_sessions_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "chat_messages" ADD CONSTRAINT "chat_messages_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "chat_sessions"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "enhancement_sessions" ADD CONSTRAINT "enhancement_sessions_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "enhancement_suggestions" ADD CONSTRAINT "enhancement_suggestions_session_id_fkey" FOREIGN KEY ("session_id") REFERENCES "enhancement_sessions"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "jobs" ADD CONSTRAINT "jobs_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "token_usages" ADD CONSTRAINT "token_usages_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "token_usages" ADD CONSTRAINT "token_usages_document_id_fkey" FOREIGN KEY ("document_id") REFERENCES "documents"("id") ON DELETE SET NULL ON UPDATE CASCADE;

