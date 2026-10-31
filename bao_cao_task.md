# 📊 BẢNG QUẢN LÝ TASK BACKEND (`auto-research-be`) — PHÂN TÁCH VAI TRÒ (JIRA ROADMAP)

> **Mục tiêu:** Phân chia rạch ròi giữa **Task Nòng Cốt (TechLead / Bạn trực tiếp nắm)** và **Task Nghiệp Vụ (Giao Member thực hiện)** để tránh việc member đụng vào các luồng nhạy cảm/phức tạp gây hỏng hệ thống.  
> **Quy ước trạng thái:** `✅ Done` \| `🪲 BUG` \| `🔄 In Progress` \| `⏳ Pending` \| `🚫 Blocked`

---

## 🛡️ BẢNG 1: TASK NÒNG CỐT & HẠ TẦNG (BẠN / TECHLEAD CHỦ ĐỘNG THỰC HIỆN)
> *Bao gồm: Hạ tầng, kết nối mạng, streaming SSE, thuật toán toán học OLS, multi-turn resume LangGraph `__interrupt__`, DevOps release, script E2E.*

| # | Task Nòng Cốt (Event) | Trạng thái | 📤 Payload Gửi sang AI (Request Fields) | 📥 Payload Nhận về từ AI (Response Fields) | 🎯 Tiêu chí Test & Nghiệm thu (DoD) |
|:---:|---|:---:|---|---|---|
| **BE-001** | **[Hạ tầng] Cấu hình IP Tailscale / Local** | `✅ Done` | `KAFKA_BROKERS`, `REDIS_HOST`, `MONGO_URI`, `MINIO_ENDPOINT`, `QDRANT_URL` trong `.env` | Kết nối TCP socket thành công, Ping/Pong phản hồi `PONG` | BE khởi động không còn log `ETIMEDOUT`; kết nối Kafka, Redis, Mongo thông suốt 100% |
| **BE-002** | **[Hạ tầng] Đồng bộ `AI_DEFAULT_MODEL`** | `✅ Done` | Envelope: `model_id: "gemini-3.5-flash-lite"` | AI xử lý và phản hồi đúng `model_id: "gemini-3.5-flash-lite"` | BE log in đúng model chuẩn, không bị cảnh báo `Model not allowed` hay lỗi 601 |
| **BE-003** | **[Hạ tầng] Nới Timeout Manager** | `✅ Done` | Config `TimeoutManagerService` theo từng `eventType` | N/A (Internal BE) | `write_content`, `writer`, `chuyen_de_*` = 1500s; `get_journals` = 900s; `analyzer`, `gen_slides` = 300s. Không ngắt sớm sau 300s |
| **BE-004** | **[Hạ tầng] Webhook Health-Check** | `✅ Done` | AI callback `PUT /health-check/:id`: `{processStatus: 1-5, message?: str}` | BE phản hồi HTTP 200: `{status: "ok", statusCode: 200}` | Gọi webhook trả về `200 OK`, `Job` trong DB cập nhật đúng tiến độ 1-5 |
| **BE-005** | **[Hạ tầng] Database Prisma Schema** | `✅ Done` | Schema Prisma: `User`, `Job`, `Document`, `Proposal`, `Outline`, `Reference`, `DataAnalysis`, `Slide` | Migration Postgres thành công | Chạy `npx prisma db push` 0 lỗi; CRUD qua Prisma Client không crash |
| **BE-006** | **[Hạ tầng] Kafka Envelope Chuẩn** | `✅ Done` | Bắt buộc 5 trường: `user_id, document_id, session_id, event_type, model_id, language` | AI echo lại: `user_id, document_id, session_id, event_type, input_tokens, output_tokens` | Gửi message qua Kafka có đủ 5 trường định danh; Redis lưu đúng key session correlate |
| **BE-027** | **[Write Core] `write_content`** | `✅ Done` | **Required:** `field, domain, references_style, final_proposal, papers_search, outline, outline_code`<br>**Optional:** `thread_id, resume, retry` | Chunks stream: `{section_name: str, content: str, references: list, has_finished: bool, thread_id: str}` | Kết nối SSE, stream nhận liên tục các section; khi xong lưu báo cáo hoàn chỉnh vào DB |
| **BE-028** | **[Write Core] `analyzer` (Hồi quy OLS)** | `✅ Done` | **Required:** `final_proposal: dict, data: dict` ({dataset_url, data_raw}), `variables: list[dict]` (BẮT BUỘC: `role: "dependent"` cho biến Y, `role: "independent"` cho biến X, `variable_type: "observed"`)<br>**Optional:** `auto_plan_mode: bool, auto_action_mode: bool, thread_id: str` | `report_html: str, analysis_results: dict, charts: list[dict], summary: str, thread_id: str, has_finished: bool` | DTO gửi sang AI có `"role": "dependent"` cho biến Y; AI tính toán thành công trả về HTML & biểu đồ charts, không crash Python |
| **BE-030** | **[Write Core] `chuyen_de_1` (Multi-turn)** | `✅ Done` | **Turn 1:** Cùng payload như `write_content`<br>**Turn 2 (Resume):** Thêm `"resumeCharacter": true`, `thread_id: document_id` | **Turn 1:** Stream ngắt với `{__interrupt__: true, chapter_break: true}`<br>**Turn 2:** Stream tiếp đến `{has_finished: true}` | Turn 1 trả về cờ ngắt; gọi `POST /reports/:id/resume` với `resumeCharacter: true` AI viết tiếp đến 100% |
| **BE-032** | **[Write Core] `chuyen_de_3` (OLS ➔ Solution)** | `✅ Done` | `field, domain, references_style, final_proposal, outline, outline_code, data: dict, variables: list[dict]` (lấy kết quả OLS từ `analyzer`) | Chunks stream giải pháp có trích dẫn bảng OLS: `{section_name, content, tables, has_finished}` | BE tự đọc kết quả OLS từ DB `DataAnalysis` gửi sang, AI sinh giải pháp bám sát số liệu |
| **BE-034** | **[Write Core] `propose_method` (2 lượt)** | `✅ Done` | **Turn 1:** `final_proposal: dict, research_type: str, auto_mode: bool`<br>**Turn 2:** `resume_method: true, user_input: str` | **Turn 1:** `ai_message: str, has_finished: false`<br>**Turn 2:** `final_proposal: dict, has_finished: true` | Lượt 1 AI đề xuất các phương pháp; lượt 2 user chọn, AI chốt hướng dẫn triển khai chi tiết |
| **BE-044** | **[Chatbot Core] `writer` (Streaming)** | `✅ Done` | **Required:** `conv_id, document_id, domain, field, summary, outline`<br>**Optional:** `notes, chat_history, use_web_search` | Chunks stream: `{change_section: bool, index: int, section: dict, has_finished: bool}` | Stream token viết bài dài mượt mà, hiển thị trực tiếp lên editor không drop token |
| **BE-049** | **[E2E] Script Test Tự Động 5 bước** | `✅ Done` | Script test tự động tuần tự: Tạo đề tài ➔ Đề cương ➔ OLS ➔ Viết bài ➔ Tải Slide PPTX | Console log báo cáo kết quả 5/5 luồng pass | Chạy `npm run test:e2e` hoàn thành 100% từ đầu đến cuối không vướng lỗi |
| **BE-050** | **[E2E] Dọn Lỗi Type & Unit Tests** | `✅ Done` | `npx tsc --noEmit` & Jest test runner | 0 lỗi compilation, 60/60 test suites PASS 100% (442/442 tests) | Chạy `npx tsc --noEmit` đạt 0 lỗi, toàn bộ test unit pass 100% sạch sẽ |
| **BE-051** | **[DevOps] Đóng Gói Docker Release** | `⏳ Pending` | Dockerfile & Docker Compose hoàn chỉnh cho BE | Container start healthy, port 3000 mapping chuẩn | Chạy `docker compose up --build` cho BE, container healthy và gọi `GET /api/health` trả về `200 OK` |

---

## 👥 BẢNG 2: TASK PHÂN HỆ & NGHIỆP VỤ (GIAO CHO MEMBER THỰC HIỆN THEO TIÊU CHÍ DOD)
> *Bao gồm: Các API độc lập, CRUD, bóc tách tài liệu, search, chatbot cơ bản, dịch thuật, gợi ý đề tài. Member chỉ cần gửi đúng Request và nhận đúng Response.*

| # | Task Phân Hệ (Event) | Trạng thái | 📤 Payload Gửi sang AI (Request Fields) | 📥 Payload Nhận về từ AI (Response Fields) | 🎯 Tiêu chí Test & Nghiệm thu (DoD) |
|:---:|---|:---:|---|---|---|
| **BE-007** | **[Doc Setup] `search_papers`** | `✅ Done` | **Required:** `field, domain, country_filter: list, query: str`<br>**Optional:** `subdomains: list, proposal: dict, oa_filter, is_oa, cut_off_year_low/high, search_range, downloaded_papers: list` | `papers: list[dict]` (mỗi item gồm: `title, user_language_title, authors, year, venue, citation, openalex_id, doi, url, q, abstract`), `is_tool: bool` | `POST /document-setup/search-papers` trả về 200 OK, mảng `papers` có tối thiểu 1 item đủ metadata |
| **BE-008** | **[Doc Setup] `get_journals`** | `🪲 BUG` | **Required:** `last_updated: str` (vd: "2024" hoặc "2025") | `message: str, last_updated: str, journals_cached: bool` | Chạy sau >300s nhận object `{message, last_updated}` không crash, BE không ngắt kết nối trước |
| **BE-009** | **[Doc Setup] `generate_domains`** | `✅ Done` | **Required:** `field: str, level: str, domains_num: int, is_tool: bool` (Bắt buộc `domains_num` và `is_tool: false`, nếu thiếu AI văng lỗi 603) | `domains: list[str]` | `POST /document-setup/domains` trả về 200 OK, mảng `domains` có 5 mục thật từ Gemini |
| **BE-010** | **[Doc Setup] `generate_subdomains`** | `✅ Done` | **Required:** `field: str, domain: str, subdomains_num: int, is_tool: bool` (Bắt buộc `subdomains_num` và `is_tool: false`) | `subdomains: list[str], field: str` | `POST /document-setup/subdomains` trả về 200 OK, mảng `subdomains` phù hợp domain |
| **BE-011** | **[Doc Setup] `generate_keywords`** | `✅ Done` | **Required:** `proposal: dict` ({title, problem_statement, motivation, research_gap})<br>**Optional:** `field, domain, subdomains` | `keywords: list[str]` | `POST /document-setup/keywords` trả về 200 OK, có tối thiểu 5 từ khóa liên quan |
| **BE-012** | **[Doc Setup] `generate_titles`** | `✅ Done` | **Required:** `field: str, domain: str, subdomains: list, level: str` (BẮT BUỘC `level` là Enum viết hoa: `KHOA_LUAN`\|`LUAN_VAN_THAC_SI`\|`LUAN_AN_TIEN_SI`\|`TIEU_LUAN`\|`BAI_BAO_KHOA_HOC`) | `proposals: list[dict]` (mỗi item: `{title, problem_statement, motivation, research_gap}`) | `POST /document-setup/titles` trả về 200 OK, lưu mảng `proposals` vào DB |
| **BE-013** | **[Doc Setup] `generate_title_description`** | `✅ Done` | **Required:** `title: str, field: str, domain: str, level: str, knowledge_base: list[str]` (BẮT BUỘC `knowledge_base` KHÔNG ĐƯỢC RỖNG, `level` là Enum) | `proposal: dict` (`title, problem_statement, motivation, research_gap`) | `POST /document-setup/title-description` trả về 200 OK, proposal lưu vào DB |
| **BE-014** | **[Doc Setup] `generate_title_description_no_info`** | `✅ Done` | **Required:** `title: str, level: str` (BẮT BUỘC `level` là Enum viết hoa `KHOA_LUAN`...)<br>**Optional:** `use_web_search: bool` | `proposal: dict, field: str, domain: str, subdomains: list[str]` | User tự gõ đề tài, API tự bóc tách field, domain, subdomains và proposal |
| **BE-015** | **[Doc Setup] `mix_titles`** | `✅ Done` | **Required:** `proposals: list[dict]`, `field: str, domain: str, level: str` (BẮT BUỘC `level` là Enum viết hoa `KHOA_LUAN`...) | `proposal: dict` (đề tài liên ngành phối hợp các ý tưởng) | `POST /document-setup/mix-titles` truyền 2 proposals trả về 1 proposal liên ngành |
| **BE-016** | **[Doc Setup] `get_research_type`** | `✅ Done` | **Required:** `proposal: dict, field: str, domain: str` | `research_type: dict` ({type: 0\|1\|2, reason: str}) | `POST /document-setup/research-type` trả về object lý do chọn định tính/định lượng |
| **BE-017** | **[Doc Setup] `generate_outline`** | `✅ Done` | **Required:** `field, domain, final_proposal: dict, research_type: dict, headings: list[str], headings_percent: list[int]` (BẮT BUỘC `headings` và `headings_percent` KHÔNG ĐƯỢC RỖNG) | `outline: dict` (`title, outline: list[dict]` cấu trúc cây chương/mục/tiểu mục) | `POST /document-setup/outline` trả về cây JSON outline phân cấp chuẩn |
| **BE-018** | **[Doc Setup] `modify_outline`** | `✅ Done` | **Required:** `field, domain, final_proposal: dict, research_type: str, headings: list, headings_percent: list, new_headings: list, new_headings_percent: list` | `new_outline: dict` | Cây outline trả về phản ánh chính xác các mục được thêm/sửa/xóa |
| **BE-019** | **[Doc Setup] `generate_outline_with_refs`** | `🔄 In Progress` | **Required:** `final_proposal: dict, outline: dict, processed_docs: list[dict]`<br>**Optional:** `outline_code: list` | `outline: dict` (từng heading trong cây outline có trường `references: list[dict]`) | Outline trả về có mảng `references[]` trong từng heading; DB lưu trọn vẹn |
| **BE-020** | **[Doc Setup] `update_user_refs`** | `⏳ Pending` | **Required:** `document_id: str, documents: list[dict]` (mapping `{heading_id, references}`) | `{status: "Success"}` | User kéo thả tài liệu vào mục, API lưu đúng tài liệu người dùng chọn vào Postgres |
| **BE-021** | **[Doc Setup] `user_documents`** | `✅ Done` | **Required:** `document_id: str, documents: list[dict]` (`file_path` trên MinIO, `file_name`) | `processed_docs: list[dict]` (mỗi doc có `content_extracted: str, file_id, file_name`) | Upload PDF lên MinIO, nhận về chuỗi text trích xuất không rỗng từ file |
| **BE-022** | **[Doc Setup] `embed_user_documents`** | `✅ Done` | **Required:** `document_id: str, processed_docs: list[dict]` | `{status: "Completed", embedded_count: int}` | Gọi API trả về completed, kiểm tra Qdrant thấy số lượng vector points tăng lên |
| **BE-023** | **[Doc Setup] `search_user_documents`** | `✅ Done` | **Required:** `document_id: str, documents: list[dict]`<br>**Optional:** `query: str, top_k: int` (default 5) | `results: list[dict]` (`text, score, page, chunk_id, document_id`) | Truy vấn semantic trả về top_k kết quả sát nhất với câu hỏi |
| **BE-024** | **[Doc Setup] `delete_documents`** | `✅ Done` | **Required:** `collection: str, data_id: str, documents: list[str]` | `data_id: str, status: "Success"` | Xóa tài liệu cá nhân: DB mất bản ghi, MinIO mất file và Qdrant mất points |
| **BE-025** | **[Doc Setup] `admin_documents`** | `✅ Done` | **Required:** `documents: list[dict], documents_urls: list[str]`<br>**Optional:** `university: str` | `processed_docs: list[dict]` | Upload biểu mẫu trường, file lưu đúng bucket `admin` trên MinIO |
| **BE-026** | **[Doc Setup] `user_guide_documents`** | `✅ Done` | **Required:** `data_id: str, documents_urls: list[str]` | `data_id: str, processed_docs: list[dict]` | Upload hướng dẫn sử dụng, phân loại đúng `guide_type` |
| **BE-029** | **[Write] `analyzer_tool`** | `⏳ Pending` | **Required:** `final_proposal: dict, data: dict, variables: list[dict], tool_configs: list[dict]` | `tool_results: dict, is_tool: true, has_finished: bool` | Nhận kết quả thống kê mô tả / correlation matrix JSON chuẩn |
| **BE-031** | **[Write] `chuyen_de_2` (Thực trạng)** | `✅ Done` | Cùng payload như `chuyen_de_1` + số liệu thực tiễn (practical data) lồng trong proposal/outline | Chunks stream thực trạng: `{section_name, content, references, has_finished}` | AI trả về nội dung chuyên đề 2 có lồng ghép bảng số liệu thực trạng |
| **BE-033** | **[Write] `gen_slides` (PowerPoint)** | `✅ Done` | **Required:** `raw_report: str`<br>**Optional:** `user_preference: int` (default 15), `manual_mode: bool`, `language: str` | `pptx_url: str, slides: list[dict]` ({title, bullets, speaker_notes}), `status: "Completed", has_finished: bool` | Nhận `pptx_url`, click vào link tải được file `.pptx` mở bằng PowerPoint |
| **BE-035** | **[Write] `comment_data`** | `✅ Done` | **Required:** `data: dict, variables: list[dict], final_proposal: dict` | `comments: list[str]` (đánh giá cỡ mẫu, độ tin cậy) | Gửi biến và dataset, nhận về mảng comments nhận xét dữ liệu |
| **BE-036** | **[Write] `check_logic_data`** | `✅ Done` | **Required:** `variables: list[dict]` | `errors_logic: list[dict], warnings_logic: list[dict]` | Trả về cảnh báo nếu biến âm, đa cộng tuyến hoặc thang đo xung đột |
| **BE-037** | **[Write] `translate_report`** | `✅ Done` | **Required:** `document_id: str, is_outline: bool, target_language: str` ("English"\|"Vietnamese") | `translated_content: str \| dict` | Gửi markdown tiếng Việt, nhận bản dịch tiếng Anh giữ nguyên format |
| **BE-038** | **[Write] `edit_report`** | `✅ Done` | **Required:** `section_name: str, content: str, instruction: str, context: str` | `content: str, section_name: str` | Gửi đoạn văn + instruction "viết súc tích hơn", nhận lại content đã chỉnh sửa |
| **BE-039** | **[Write] `parsed_value`** | `✅ Done` | **Required:** `survey_questions: list[dict]` ({variable_name, variable_code, variable_type, measurement_type, questions, answer_content}), `variables: list[dict]` | `parsed_variables: list[dict]` | Gửi bảng câu hỏi khảo sát Likert, nhận về danh sách biến định lượng chuẩn hóa |
| **BE-040** | **[Write] `delete_report`** | `✅ Done` | **Required:** `document_id: str, thread_id: str` | `failed_ids: list[str], status: "Deleted"` | Xóa báo cáo trong DB và dọn sạch file slide/report trên MinIO |
| **BE-041** | **[Chatbot] `outer_chatbot`** | `✅ Done` | **Required:** `conv_id: str, question: str, chat_history: list[dict]`<br>**Optional:** `short_answer: bool, direct_answer: bool, use_web_search_chatbot: bool` | `response: str, conv_id: str, functionality_error: str` | Hỏi đáp học thuật, nhận câu trả lời kèm token tiêu thụ |
| **BE-042** | **[Chatbot] `inner_chatbot`** | `✅ Done` | **Required:** `conv_id: str, document_id: str, question: str, chat_history: list[dict]`<br>**Optional:** `short_answer, direct_answer, use_web_search_chatbot` | `response: str, conv_id: str, cited_sources: list[dict]` | Hỏi tài liệu đã nạp, câu trả lời kèm trích dẫn `cited_sources` đúng trang |
| **BE-043** | **[Chatbot] `writer_chatbot`** | `✅ Done` | **Required:** `conv_id: str, question: str`<br>**Optional:** `resume: bool, template_key: str, short_answer: bool, use_web_search_chatbot: bool` | `ai_message: str, done_chat: bool, summary: str, outline: dict, template_key: str, chat_history: list` | Chat đa lượt; khi `done_chat: true`, DB lưu đủ `summary` và `outline` |
| **BE-045** | **[Enhance] `ai_in_doc`** | `✅ Done` | **Required:** `document_id: str, section_id: str, paragraphs: str, paragraph_context: str, action: str` ("rewrite"\|"expand"\|"shorten"\|"fix_grammar"), `proposal: dict` | `response: str, section_id: str` | Bôi đen đoạn văn chọn action `fix_grammar`, nhận lại đoạn văn đã sửa đúng `section_id` |
| **BE-046** | **[Enhance] `get_suggestions`** | `✅ Done` | **Required:** `document_id: str, section: str, section_context: str, processed_docs: list[dict]` | `suggested_paragraphs: list[dict]` ({original_paragraph, suggested_paragraph, explanation, type}), `disable_edit: bool` | Gọi API nhận mảng `suggested_paragraphs` lưu vào DB `EnhancementSuggestion` |
| **BE-047** | **[Enhance] `enhance`** | `✅ Done` | **Required:** `document_id: str, section: str, suggested_paragraph: dict` | `updated_paragraphs: str \| list[str]` | User bấm áp dụng 1 suggestion, đoạn văn trong DB được cập nhật nội dung mới |
| **BE-048** | **[Enhance] `enhance_all`** | `✅ Done` | **Required:** `document_id: str, section: str, suggested_paragraphs: list[dict]` | `updated_paragraphs: str \| list[str]` | User bấm áp dụng tất cả, toàn bộ văn bản được cập nhật một lượt |
| **BE-049** | **[Pipeline E2E] Kiểm thử 5 bước tích hợp** | `✅ Done` | **Required:** Full flow từ `generate_title_description` ➔ `generate_outline` ➔ `analyzer` ➔ `chuyen_de_1` ➔ `gen_slides` | Test Suite E2E pass 100%, verify luồng nghiệp vụ 5 bước chạy trơn tru | Hoàn thành script verify & Jest E2E test `test/pipeline-5steps.e2e-spec.ts` (5/5 PASS) |
| **BE-050** | **[Fix Type & Unit Test] Fix Timeout 900s & Lỗi Type** | `✅ Done` | **Required:** Dọn dẹp lỗi type, fix test timeout `admin-documents-ai.service.spec.ts` (900s `get_journals`), đảm bảo `tsc --noEmit` pass | Toàn bộ Unit test & Build pass 100% | Đã fix mock timer 900s, 14/14 unit test PASS trong 0.6s, 0 lỗi biên dịch |

---

## 🪲 BẢNG 3: TASK BUG BỔ SUNG CỦA BACKEND (BE XỬ LÝ TRỰC TIẾP)
> *Các bug phát hiện qua kiểm thử thực tế với AI Server: lỗi lệch contract DTO, thiếu field validate, timeout cần nới.*

| # | Mã Bug | Tên Bug & Nghiệp Vụ | Nguyên Nhân & Hiện Tượng | Giải Pháp Khắc Phục Cụ Thể | Ưu Tiên |
|:---:|---|---|---|---|:---:|
| **1** | **BE-BUG-01** | **Lệch Enum `level` ở cụm Đề tài** (`BE-012`, `BE-014`, `BE-015`) | Gửi `level: "dai_hoc"` bị AI văng lỗi 603 (`Invalid level: 'dai_hoc'`). AI Server bắt buộc là Enum viết hoa: `BAI_BAO_KHOA_HOC`, `KHOA_LUAN`, `LUAN_VAN_THAC_SI`, `LUAN_AN_TIEN_SI`, `TIEU_LUAN` | Sử dụng helper `normalizeAcademicLevel(dto.level)` trong `proposal-ideation.service.ts` và `proposal-details.service.ts` trước khi dispatch sang Kafka | `🔴 High` |
| **2** | **BE-BUG-02** | **Cấu trúc `knowledge_base` bắt buộc là `list[dict]`** (`BE-013`) | AI Server văng lỗi 603: `knowledge_base cannot be empty` (nếu gửi `[]`) và `knowledge_base must be a dictionary` (nếu gửi `list[str]`). AI bắt buộc là mảng các object: `list[dict]` | Trong `proposal-details.service.ts`, format `knowledge_base` thành: `[ { title: dto.title, content: 'Tài liệu tham khảo cơ sở đề tài' } ]` (mảng các object) | `🔴 High` |
| **3** | **BE-BUG-03** | **Cấu trúc `headings` bắt buộc là `list[dict]` & `word_count_str`** (`BE-017`) | AI Server văng lỗi 603 nếu gửi `headings: []` (`cannot be empty`) hoặc `headings: list[str]` (`must be a dictionary`). Ngoài ra AI gọi `int(word_count_str)` nên nếu gửi `'8000 words'` sẽ bị văng `ValueError` | Trong `outline-ai.service.ts`: format `headings: [{ title: '...', percent: 20 }]`, và gửi `word_count_str: '8000'` (chuỗi số thuần túy) | `🔴 High` |
| **4** | **BE-BUG-04** | **DTO Webhook Health-Check ép cứng `eventType`** (`BE-004`) | AI Server callback `PUT /health-check/:id` chỉ gửi `{processStatus, message}` mà không gửi `eventType`, bị NestJS chặn lỗi 400 Bad Request | Đổi `@IsNotEmpty()` thành `@IsOptional()` cho `eventType` trong `HealthCheckWebhookDto`, nếu thiếu thì BE tự lấy từ DB `Job.action` | `🟡 Medium` |
| **5** | **BE-BUG-05** | **Nới Timeout Manager cho cụm Đề tài & Sinh bài** (`BE-003`) | Khi Gemini sinh danh sách nhiều đề tài dài (`generate_titles`) hoặc outline, thời gian xử lý mất 70s-90s, vượt quá timeout 60s hiện tại | Nới timeout cho `generate_titles`, `generate_outline`, `search_papers` lên 120s - 180s trong `TimeoutManagerService` | `🟡 Medium` |
| **6** | **BE-BUG-06** | **Thiếu trường `language: 'vi'` ở Chatbot** (`BE-041`, `BE-042`) | Gửi request `outer_chatbot` sang topic `chatbot_request` thiếu `language` bị AI văng lỗi 603: `Missing field 'language'` | Bổ sung mặc định `language: dto.language || 'vi'` vào payload dispatch sang `chatbot_request` | `🔴 High` |

---

## 🤖 BẢNG 4: TASK BUG BỔ SUNG CỦA SERVER AI (GỬI ĐỘI AI / DEVOPS XỬ LÝ TRÊN VPS)
> *Các sự cố phát hiện ở tầng Worker Python AI Server trên VPS: lỗi crash process, sai tên Enum exception, nghẽn mạng.*

| # | Mã Bug | Phân Hệ AI Bị Lỗi | Hiện Tượng & Chi Tiết Lỗi Trả Về | Hướng Dẫn Đội AI Khắc Phục Trên VPS | Trạng Thái |
|:---:|---|---|---|---|:---:|
| **1** | **AI-BUG-01** | **Worker Chatbot phản hồi chậm / Timeout** (`BE-041`, `BE-042`) | Worker đã lắng nghe `chatbot_request`, yêu cầu bắt buộc `language: 'vi'`, nhưng khi xử lý prompt thì mất quá 30s-40s hoặc timeout | Kiểm tra tài nguyên và log của `chatbot_worker.py` trên VPS xem có bị nghẽn prompt hay chờ Gemini không | `🟡 Medium` |
| **2** | **AI-BUG-02** | **Worker Enhancement** (`BE-045`) | Gửi event `ai_in_doc` vào topic `enhancement_request` bị timeout | Kiểm tra process `enhancement_worker.py` trên VPS, kiểm tra kết nối Kafka Consumer group `chatbot_09_06_2025` | `🔴 Blocking` |
| **3** | **AI-BUG-03** | **Crash Python `delete_report`** (`BE-040`) | Nhận tin nhắn nhưng crash văng lỗi 640: `type object 'WriteSectionError' has no attribute 'INVALID_PARAMS'` | Trong code Python worker `write_section`, sửa lại tên thuộc tính Exception của class `WriteSectionError` (gõ sai tên `INVALID_PARAMS` khiến worker văng Exception unhandled) | `🔴 High (Chưa fix)` |
| **4** | **AI-BUG-04** | **Cào bài báo OpenAlex Timeout** (`BE-007`) | Event `search_papers` bị treo > 60s không phản hồi | Kiểm tra script cào OpenAlex trên VPS, tối ưu timeout HTTP gọi ra ngoài internet và thêm cơ chế fallback cache | `🟡 Medium` |
| **5** | **AI-BUG-05** | **Crash Python `generate_outline`** (`BE-017`) | Khi gửi payload chuẩn đầy đủ, AI worker văng lỗi 617: `list index out of range` | Mở log traceback của worker trên VPS khi nhận event `generate_outline` để xem dòng code Python nào bị `IndexError: list index out of range` (nghi ngờ code AI truy cập index cố định của một mảng con) | `🔴 High (Mới)` |

---

### 📈 TỔNG KẾT TIẾN ĐỘ THỰC TẾ (CẬP NHẬT SAU RE-TEST THỰC TẾ)

```
┌────────────────────────────────────────────────────────────┐
│                    TIẾN ĐỘ BACKEND HIỆN TẠI                │
├──────────────────────────┬──────────────────┬──────────────┤
│ Phân nhóm                │ Số lượng task    │ Tỷ lệ (%)    │
├──────────────────────────┼──────────────────┼──────────────┤
│ 🛡️ Bảng 1: Bạn/TechLead  │ 15 tasks         │ 24.2%        │
│ 👥 Bảng 2: Giao Member   │ 36 tasks         │ 58.0%        │
│ 🪲 Bảng 3: Bug mới của BE│ 6 bug tasks      │ 9.7%         │
│ 🤖 Bảng 4: Bug phía AI   │ 5 bug tasks      │ 8.1%         │
├──────────────────────────┼──────────────────┼──────────────┤
│ 👉 TỔNG CỘNG             │ 62 micro-tasks   │ 100% Hệ thống│
└──────────────────────────┴──────────────────┴──────────────┘
```

---

### 🎯 THỨ TỰ HÀNH ĐỘNG TIẾP THEO:

1. **Xử lý nhanh Bảng 3 (Backend):**
   - Sửa `BE-BUG-01` (`normalizeAcademicLevel`), `BE-BUG-02` (format `knowledge_base` sang `list[dict]`), `BE-BUG-03` (format `headings` sang `list[dict]` và `word_count_str` là chuỗi số), `BE-BUG-04` (Optional `eventType`), `BE-BUG-05` (nới timeout), `BE-BUG-06` (bổ sung `language: 'vi'` cho chatbot).
2. **Gửi Bảng 4 cho Đội AI / DevOps:**
   - Báo đội AI sửa bug `AI-BUG-03` (`WriteSectionError.INVALID_PARAMS`) và mở log traceback kiểm tra lỗi `AI-BUG-05` (`list index out of range` ở hàm `generate_outline`).