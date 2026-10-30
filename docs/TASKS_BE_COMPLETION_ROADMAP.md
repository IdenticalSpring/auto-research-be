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
| **BE-003** | **[Hạ tầng] Nới Timeout Manager** | `🪲 BUG` | Config `TimeoutManagerService` theo từng `eventType` | N/A (Internal BE) | `write_content`, `writer`, `chuyen_de_*` = 1500s; `get_journals` = 900s; `analyzer`, `gen_slides` = 300s. Không ngắt sớm sau 300s |
| **BE-004** | **[Hạ tầng] Webhook Health-Check** | `✅ Done` | AI callback `PUT /health-check/:id`: `{processStatus: 1-5, message?: str}` | BE phản hồi HTTP 200: `{status: "ok", statusCode: 200}` | Gọi webhook trả về `200 OK`, `Job` trong DB cập nhật đúng tiến độ 1-5 |
| **BE-005** | **[Hạ tầng] Database Prisma Schema** | `✅ Done` | Schema Prisma: `User`, `Job`, `Document`, `Proposal`, `Outline`, `Reference`, `DataAnalysis`, `Slide` | Migration Postgres thành công | Chạy `npx prisma db push` 0 lỗi; CRUD qua Prisma Client không crash |
| **BE-006** | **[Hạ tầng] Kafka Envelope Chuẩn** | `✅ Done` | Bắt buộc 5 trường: `user_id, document_id, session_id, event_type, model_id, language` | AI echo lại: `user_id, document_id, session_id, event_type, input_tokens, output_tokens` | Gửi message qua Kafka có đủ 5 trường định danh; Redis lưu đúng key session correlate |
| **BE-027** | **[Write Core] `write_content`** | `✅ Done` | **Required:** `field, domain, references_style, final_proposal, papers_search, outline, outline_code`<br>**Optional:** `thread_id, resume, retry` | Chunks stream: `{section_name: str, content: str, references: list, has_finished: bool, thread_id: str}` | Kết nối SSE, stream nhận liên tục các section; khi xong lưu báo cáo hoàn chỉnh vào DB |
| **BE-028** | **[Write Core] `analyzer` (Hồi quy OLS)** | `✅ Done` | **Required:** `final_proposal: dict, data: dict` ({dataset_url, data_raw}), `variables: list[dict]` (BẮT BUỘC: `role: "dependent"` cho biến Y, `role: "independent"` cho biến X, `variable_type: "observed"`)<br>**Optional:** `auto_plan_mode: bool, auto_action_mode: bool, thread_id: str` | `report_html: str, analysis_results: dict, charts: list[dict], summary: str, thread_id: str, has_finished: bool` | DTO gửi sang AI có `"role": "dependent"` cho biến Y; AI tính toán thành công trả về HTML & biểu đồ charts, không crash Python |
| **BE-030** | **[Write Core] `chuyen_de_1` (Multi-turn)** | `✅ Done` | **Turn 1:** Cùng payload như `write_content`<br>**Turn 2 (Resume):** Thêm `"resumeCharacter": true`, `thread_id: document_id` | **Turn 1:** Stream ngắt với `{__interrupt__: true, chapter_break: true}`<br>**Turn 2:** Stream tiếp đến `{has_finished: true}` | Turn 1 trả về cờ ngắt; gọi `POST /reports/:id/resume` với `resumeCharacter: true` AI viết tiếp đến 100% |
| **BE-032** | **[Write Core] `chuyen_de_3` (OLS ➔ Solution)** | `✅ Done` | `field, domain, references_style, final_proposal, outline, outline_code, data: dict, variables: list[dict]` (lấy kết quả OLS từ `analyzer`) | Chunks stream giải pháp có trích dẫn bảng OLS: `{section_name, content, tables, has_finished}` | BE tự đọc kết quả OLS từ DB `DataAnalysis` gửi sang, AI sinh giải pháp bám sát số liệu |
| **BE-034** | **[Write Core] `propose_method` (2 lượt)** | `✅ Done` | **Turn 1:** `final_proposal: dict, research_type: str, auto_mode: bool`<br>**Turn 2:** `resume_method: true, user_input: str` | **Turn 1:** `ai_message: str, has_finished: false`<br>**Turn 2:** `final_proposal: dict, has_finished: true` | Lượt 1 AI đề xuất các phương pháp; lượt 2 user chọn, AI chốt hướng dẫn triển khai chi tiết |
| **BE-044** | **[Chatbot Core] `writer` (Streaming)** | `✅ Done` | **Required:** `conv_id, document_id, domain, field, summary, outline`<br>**Optional:** `notes, chat_history, use_web_search` | Chunks stream: `{change_section: bool, index: int, section: dict, has_finished: bool}` | Stream token viết bài dài mượt mà, hiển thị trực tiếp lên editor không drop token |
| **BE-049** | **[E2E] Script Test Tự Động 5 bước** | `⏳ Pending` | Script test tự động tuần tự: Tạo đề tài ➔ Đề cương ➔ OLS ➔ Viết bài ➔ Tải Slide PPTX | Console log báo cáo kết quả 5/5 luồng pass | Chạy `npm run test:e2e` hoàn thành 100% từ đầu đến cuối không vướng lỗi |
| **BE-050** | **[E2E] Dọn Lỗi Type & Unit Tests** | `⏳ Pending` | `npx tsc --noEmit` & Jest test runner | 0 lỗi compilation, Jest coverage report >= 90% | Chạy `npx tsc --noEmit` đạt 0 lỗi, `npm run test:cov` hiển thị coverage >= 90% |
| **BE-051** | **[DevOps] Đóng Gói Docker Release** | `⏳ Pending` | Dockerfile & Docker Compose hoàn chỉnh cho BE | Container start healthy, port 3000 mapping chuẩn | Chạy `docker compose up --build` cho BE, container healthy và gọi `GET /api/health` trả về `200 OK` |

---

## 👥 BẢNG 2: TASK PHÂN HỆ & NGHIỆP VỤ (GIAO CHO MEMBER THỰC HIỆN THEO TIÊU CHÍ DOD)
> *Bao gồm: Các API độc lập, CRUD, bóc tách tài liệu, search, chatbot cơ bản, dịch thuật, gợi ý đề tài. Member chỉ cần gửi đúng Request và nhận đúng Response.*

| # | Task Phân Hệ (Event) | Trạng thái | 📤 Payload Gửi sang AI (Request Fields) | 📥 Payload Nhận về từ AI (Response Fields) | 🎯 Tiêu chí Test & Nghiệm thu (DoD) |
|:---:|---|:---:|---|---|---|
| **BE-007** | **[Doc Setup] `search_papers`** | `✅ Done` | **Required:** `field, domain, country_filter: list, query: str`<br>**Optional:** `subdomains: list, proposal: dict, oa_filter, is_oa, cut_off_year_low/high, search_range, downloaded_papers: list` | `papers: list[dict]` (mỗi item gồm: `title, user_language_title, authors, year, venue, citation, openalex_id, doi, url, q, abstract`), `is_tool: bool` | `POST /document-setup/search-papers` trả về 200 OK, mảng `papers` có tối thiểu 1 item đủ metadata |
| **BE-008** | **[Doc Setup] `get_journals`** | `🪲 BUG` | **Required:** `last_updated: str` (vd: "2024" hoặc "2025") | `message: str, last_updated: str, journals_cached: bool` | Chạy sau >300s nhận object `{message, last_updated}` không crash, BE không ngắt kết nối trước |
| **BE-009** | **[Doc Setup] `generate_domains`** | `✅ Done` | **Required:** `field: str, level: str` ("dai_hoc"\|"thac_si"\|"tien_si"\|"hoc_sinh"\|"chuyen_gia") | `domains: list[str]` | `POST /document-setup/domains` với `level: "dai_hoc"` trả về 200 OK, mảng `domains` không rỗng |
| **BE-010** | **[Doc Setup] `generate_subdomains`** | `✅ Done` | **Required:** `field: str, domain: str`<br>**Optional:** `subdomains_num: int` (default 5), `is_tool: bool` | `subdomains: list[str], field: str, is_tool: bool` | `POST /document-setup/subdomains` trả về 200 OK, mảng `subdomains` phù hợp domain |
| **BE-011** | **[Doc Setup] `generate_keywords`** | `✅ Done` | **Required:** `proposal: dict` ({title, problem_statement, motivation, research_gap})<br>**Optional:** `field, domain, subdomains` | `keywords: list[str]` (hoặc dict `{keywords_main: list, keywords_sub: list}`) | `POST /document-setup/keywords` trả về 200 OK, có tối thiểu 5 từ khóa liên quan |
| **BE-012** | **[Doc Setup] `generate_titles`** | `✅ Done` | **Required:** `field: str, domain: str, subdomains: list, level: str`<br>**Optional:** `use_web_search: bool` | `proposals: list[dict]` (mỗi item: `{title, problem_statement, motivation, research_gap}`), `knowledge_base: str` | `POST /document-setup/titles` trả về 200 OK, lưu mảng `proposals` vào bảng `Proposal` trong DB |
| **BE-013** | **[Doc Setup] `generate_title_description`** | `✅ Done` | **Required:** `title: str, field: str, domain: str, level: str`<br>**Optional:** `knowledge_base: str` | `proposal: dict` (`title, problem_statement, motivation, research_gap`) | `POST /document-setup/title-description` trả về 200 OK, proposal lưu vào bảng `Proposal` |
| **BE-014** | **[Doc Setup] `generate_title_description_no_info`** | `✅ Done` | **Required:** `title: str, level: str`<br>**Optional:** `use_web_search: bool` | `proposal: dict, field: str, domain: str, subdomains: list[str]` | User tự gõ đề tài, API tự bóc tách field, domain, subdomains và proposal chi tiết |
| **BE-015** | **[Doc Setup] `mix_titles`** | `✅ Done` | **Required:** `proposals: list[dict]` (2+ đề tài muốn phối hợp), `field: str, domain: str, level: str` | `proposal: dict` (đề tài liên ngành phối hợp các ý tưởng) | `POST /document-setup/mix-titles` truyền 2 proposals trả về 1 proposal liên ngành hoàn chỉnh |
| **BE-016** | **[Doc Setup] `get_research_type`** | `✅ Done` | **Required:** `proposal: dict, field: str, domain: str` | `research_type: str` ("Định lượng" \| "Định tính" \| "Hỗn hợp") | `POST /document-setup/research-type` trả về 1 trong 3 chuỗi phương pháp luận |
| **BE-017** | **[Doc Setup] `generate_outline`** | `✅ Done` | **Required:** `field, domain, final_proposal: dict, research_type: str`<br>**Optional:** `outline_code: list, headings: list, headings_percent: list, word_count_str: str` | `outline: dict` (`title, outline: list[dict]` cấu trúc cây chương/mục/tiểu mục), `final_proposal: dict` | `POST /document-setup/outline` trả về cây JSON outline phân cấp chuẩn |
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

### 📈 TỔNG KẾT TIẾN ĐỘ THỰC TẾ THEO 2 BẢNG

```
┌────────────────────────────────────────────────────────────┐
│                    TIẾN ĐỘ BACKEND HIỆN TẠI                │
├──────────────────────────┬──────────────────┬──────────────┤
│ Phân nhóm                │ Số lượng task    │ Tỷ lệ (%)    │
├──────────────────────────┼──────────────────┼──────────────┤
│ 🛡️ Bảng 1: Bạn/TechLead  │ 15 tasks         │ 29.4%        │
│ 👥 Bảng 2: Giao Member   │ 36 tasks         │ 70.6%        │
├──────────────────────────┼──────────────────┼──────────────┤
│ 👉 TỔNG CỘNG             │ 51 micro-tasks   │ 100% BE      │
└──────────────────────────┴──────────────────┴──────────────┘
```

---

### 🎯 THỨ TỰ ƯU TIÊN THỰC HIỆN:

1. **TechLead (Bạn) xử lý 3 Bugs nòng cốt ở Bảng 1:**
   - **`BE-003`**: Nới `TimeoutManagerService` lên 1500s cho các task viết bài dài và 900s cho `get_journals`.
   - **`BE-028`**: Thêm `"role": "dependent"` vào DTO `analyzer` để hồi quy OLS không bị lỗi thiếu field.
   - **`BE-030`**: Tạo endpoint resume cho `chuyen_de_1` với `resumeCharacter: true`.
2. **Giao Member thực hiện độc lập ở Bảng 2:**
   - Hoàn thiện các task Pending/In Progress: `gen_slides` (BE-033), `edit_report` (BE-038), `parsed_value` (BE-039), `writer_chatbot` (BE-043), `enhance` (BE-047, BE-048).
   - Member tự test theo tiêu chí **DoD** và đối chiếu đúng **Request/Response Payload** trước khi báo cáo.