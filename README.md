# 🚀 AUTO-RESEARCH-BE

> Backend API service cho hệ thống Nghiên cứu Tự động (**Auto Research**), phát triển bằng **NestJS 12**, **TypeScript 6**, **Prisma ORM**, **PostgreSQL**, **Apache Kafka** và **Redis**.

---

## ⚡ HƯỚNG DẪN KHỞI CHẠY NHANH (DOCKER DEV + HOT RELOAD)

Dự án đã được tích hợp bộ script **tự động hóa 100%** (tự kiểm tra Docker, tự tạo `.env`, tự tạo Docker Network `autoresearching-net`, tự đồng bộ Database và bật Logs theo dõi Hot Reload).

### 🐧 Dành cho Linux / macOS:
Chạy **1 lệnh duy nhất** tại thư mục gốc dự án:
```bash
./scripts/dev-setup.sh
```

### 🪟 Dành cho Windows (PowerShell):
Mở PowerShell tại thư mục gốc dự án và chạy:
```powershell
.\scripts\dev-setup.ps1
```

> 💡 **Cơ chế thông minh:**
> - Nếu container **chưa chạy**: Script sẽ tự động build image dev, khởi động Postgres + Backend, chạy `prisma db push` đồng bộ schema và mở logs.
> - Nếu container **đang chạy sẵn**: Script sẽ phát hiện và chỉ bật logs lên để bạn tiếp tục code, tránh khởi tạo trùng gây xung đột.
> - **Hot Reload**: Bất kỳ thay đổi code nào trong `src/` khi bạn lưu file (`Ctrl + S`) sẽ được NestJS tự động biên dịch lại ngay lập tức trong container!

---

## 🌐 DANH SÁCH ĐƯỜNG DẪN TRUY CẬP (QUICK ACCESS URLS)

Do logs của NestJS khi khởi động ánh xạ hàng trăm route nên dễ bị trôi thông tin, dưới đây là bảng tổng hợp các địa chỉ và endpoint quan trọng:

### 1. Địa chỉ dịch vụ cốt lõi

| Dịch vụ | URL / Cổng truy cập | Thông tin kết nối / Hướng dẫn |
|---|---|---|
| **Backend API Base** | [`http://localhost:3001/api/v1`](http://localhost:3001/api/v1) | Endpoint gốc API (Health check) |
| **Prisma Studio (Web GUI)** | [`http://localhost:5555`](http://localhost:5555) | 🌐 **Xem Database trên trình duyệt web** (không cần cài phần mềm) |
| **PostgreSQL Database** | `localhost:5432` | 💻 Kết nối bằng **DBeaver, Navicat, TablePlus**<br>User: `postgres` \| Pass: `postgres` \| DB: `autoresearch` |

#### 📖 Hướng dẫn kết nối Database cho thành viên trong team:

- **Cách 1: Xem nhanh trên trình duyệt Web (Không cần cài phần mềm - Tiện lợi cho Linux)**
  - Mở trình duyệt truy cập: [`http://localhost:5555`](http://localhost:5555)
  - Toàn bộ bảng, dữ liệu sẽ hiển thị dạng bảng tính trực quan để xem, tìm kiếm và chỉnh sửa trực tiếp.

- **Cách 2: Dùng phần mềm quản trị chuyên dụng (DBeaver, TablePlus, Navicat, DataGrip)**
  1. Mở phần mềm quản lý Database yêu thích của bạn lên.
  2. Tạo kết nối mới (**New Connection**) và chọn loại database là **PostgreSQL**.
  3. Điền các thông số kết nối chuẩn sau:
     - **Host / Server**: `localhost` (hoặc `127.0.0.1`)
     - **Port**: `5432`
     - **Database**: `autoresearch`
     - **Username**: `postgres`
     - **Password**: `postgres`
  4. Bấm **Test Connection** để kiểm tra kết nối thành công, sau đó bấm **Save / Connect** để bắt đầu viết câu lệnh SQL và quản lý bảng.

---

### 2. Thông tin Hạ tầng VPS & Dịch vụ AI (Monitoring & Storage)

Toàn bộ dịch vụ Apache Kafka Broker, AI Workers và công cụ giám sát lưu trữ được triển khai tập trung trên VPS công khai:

| Dịch vụ | Địa chỉ / URL | Ghi chú & Tài khoản |
|---|---|---|
| **Máy chủ VPS** | `167.254.70.125` | IP máy chủ dịch vụ nền tảng AI |
| **Kafka Broker** | `167.254.70.125:9092` | Protocol: `PLAINTEXT` |
| **Kafka UI (Web)** | [`http://167.254.70.125:8080`](http://167.254.70.125:8080) | Giao diện Web theo dõi topics & events Kafka |
| **Local LLM API** | [`http://167.254.70.125:8000/v1`](http://167.254.70.125:8000/v1) | Model đang sử dụng: `openai/gpt-oss-120b` (OpenAI-compatible) |
| **MinIO Console (S3)** | [`http://167.254.70.125:9001`](http://167.254.70.125:9001) | User: `minio_admin` \| Pass: `minio_password_local` |
| **Qdrant Dashboard** | [`http://167.254.70.125:6333/dashboard`](http://167.254.70.125:6333/dashboard) | Vector Database Dashboard |
| **QUAN LY MODEL UI (Web)** | [`http://167.254.70.125:8088`](http://167.254.70.125:8088) | Giao diện Web quản lý model API AI | admin123

#### 📚 Tài liệu tích hợp hệ thống (Integration Documentation):
- `docs/KAFKA_INTEGRATION_GUIDE.md`: Định dạng sự kiện Kafka và đặc tả cấu trúc JSON Payload.
- `docs/FE_DOCUMENT_SETUP_TASKS.md`: Luồng nghiệp vụ Frontend và yêu cầu xử lý cấp màn hình.

---

### 3. Danh mục API Endpoints chính (`/api/v1/...`)

#### 🔐 Xác thực & Người dùng (Auth & Users)
| Method | Endpoint | Mô tả |
|---|---|---|
| `POST` | `/api/v1/auth/register` | Đăng ký tài khoản mới |
| `POST` | `/api/v1/auth/login` | Đăng nhập lấy JWT Bearer token |
| `GET` | `/api/v1/users/profile` | Xem thông tin cá nhân |
| `PATCH`| `/api/v1/users/change-password` | Đổi mật khẩu người dùng |

#### 📄 Thiết lập tài liệu nghiên cứu (Document Setup)
| Method | Endpoint | Mô tả |
|---|---|---|
| `POST / GET` | `/api/v1/documents` | Tạo mới / Lấy danh sách tài liệu nghiên cứu |
| `GET / PATCH / DELETE` | `/api/v1/documents/:id` | Xem chi tiết, cập nhật, xóa document |
| `POST / GET` | `/api/v1/proposals` | Đề xuất nghiên cứu (problem statement, motivation) |
| `POST / GET` | `/api/v1/document-keywords` | Từ khóa chính, từ khóa phụ, search queries |
| `POST / GET` | `/api/v1/references` | Tài liệu tham khảo, trạng thái tải PDF open-access |
| `POST / GET` | `/api/v1/user-uploaded-documents` | Tài liệu PDF người dùng upload và embedding |
| `POST / GET` | `/api/v1/outlines` | Cấu trúc đề cương và số lượng từ mục tiêu |

#### 📊 Soạn thảo báo cáo & Phân tích dữ liệu (Write Report)
| Method | Endpoint | Mô tả |
|---|---|---|
| `POST / GET` | `/api/v1/reports` | Nội dung báo cáo Markdown |
| `POST / GET` | `/api/v1/datasets` | Quản lý tập dữ liệu CSV/Excel tải lên |
| `POST / GET` | `/api/v1/dataset-variables` | Danh sách biến (Type, Role, Scale, Unit) |
| `POST / GET` | `/api/v1/proposed-methods` | Phương pháp đề xuất, mô hình, giả thuyết nghiên cứu |
| `POST / GET` | `/api/v1/generated-slides` | Slide thuyết trình được sinh tự động (.pptx) |

#### 💬 Trợ lý AI Chatbot
| Method | Endpoint | Mô tả |
|---|---|---|
| `POST / GET` | `/api/v1/chat-sessions` | Phiên trò chuyện (`outer_chatbot`, `inner_chatbot`, `writer_chatbot`) |
| `POST / GET` | `/api/v1/chat-messages` | Lịch sử và gửi tin nhắn trao đổi với AI |

#### ✨ Tối ưu hóa văn bản (Enhancement)
| Method | Endpoint | Mô tả |
|---|---|---|
| `POST / GET` | `/api/v1/enhancement-sessions` | Phiên cải tiến theo đoạn văn bản chọn |
| `POST / GET` | `/api/v1/enhancement-suggestions`| Gợi ý sửa lỗi ngữ pháp, viết lại, học thuật hóa |

#### ⚙️ Quản lý tác vụ bất đồng bộ (Jobs)
| Method | Endpoint | Mô tả |
|---|---|---|
| `GET` | `/api/v1/jobs` | Lấy danh sách tác vụ (hỗ trợ query `?status=...`) |
| `GET` | `/api/v1/jobs/:id` | Xem tiến độ chi tiết của một Job |
| `PATCH`| `/api/v1/jobs/:id/resume` | Tiếp tục thực thi Job bị tạm dừng từ `resumeStep` |

---

## 🛠️ CÁC LỆNH TIỆN ÍCH DÀNH CHO DEVELOPER

Bạn có thể dùng lệnh `make` hoặc `npm run`:

| Mục đích | Lệnh Makefile | Lệnh NPM |
|---|---|---|
| **Khởi động Dev (Hot Reload)** | `make up` | `npm run docker:dev` |
| **Xem logs thời gian thực** | `make logs` | `docker logs -f auto-research-be-local` |
| **Tắt môi trường Docker** | `make down` | `npm run docker:down` |
| **Đồng bộ Database (Prisma)** | `make db-push` | `npm run docker:db-push` |
| **Khởi động lại chỉ riêng Backend**| `make restart-be` | `docker restart auto-research-be-local` |
| **Mở Terminal bên trong Container**| `make sh` | `docker exec -it auto-research-be-local sh` |
| **Chạy Linter kiểm tra code** | — | `npm run lint` |
| **Format code toàn dự án** | — | `npm run format` |

---

## 🏗️ CẤU TRÚC THƯ MỤC CHI TIẾT DỰ ÁN

```text
auto-research-be/
├── README.md                  # Tài liệu hướng dẫn tổng quan dự án
├── Makefile                   # Quản lý các lệnh Docker / DB / Dev tiện ích
├── Jenkinsfile                # CI/CD Pipeline tự động hóa build/deploy
├── Dockerfile                 # Multi-stage Dockerfile cho môi trường Production
├── Dockerfile.dev             # Dockerfile tối ưu cho Dev có Hot Reload thời gian thực
├── docker-compose.prod.yml    # Compose cho môi trường Production
├── .env.example               # Template danh sách biến môi trường mẫu
├── package.json               # Quản lý dependencies và scripts chạy dự án
├── prisma/
│   └── schema.prisma          # Toàn bộ Database Models (User, Document, Job, TokenUsage, v.v.)
├── scripts/                   # Bộ scripts tiện ích tự động hóa
│   ├── dev-setup.sh           # Script 1-click khởi chạy tự động cho Linux/macOS
│   ├── dev-setup.ps1          # Script 1-click khởi chạy tự động cho Windows
│   └── smoke-test-kafka.ts    # Script E2E kiểm thử kết nối trực tiếp Kafka Broker thực tế
├── docs/                      # Tài liệu kỹ thuật dự án
│   └── backend-cicd.md        # Hướng dẫn CI/CD Jenkins & Docker
└── src/
    ├── main.ts                # Entrypoint ứng dụng (CORS, ValidationPipe, Prefix api/v1)
    ├── app.module.ts          # Root module kết nối toàn bộ hệ thống
    ├── app.controller.ts      # Health check root
    ├── app.service.ts         # Service root
    ├── config/                # Cấu hình tập trung (@nestjs/config)
    │   ├── configuration.ts   # Load và tổng hợp cấu hình
    │   ├── app.config.ts      # Port, prefix, cors, body limit
    │   ├── auth.config.ts     # JWT secret & expiration
    │   ├── database.config.ts # DATABASE_URL
    │   ├── kafka.config.ts    # Kafka brokers, topics (hỗ trợ suffix _local)
    │   └── redis.config.ts    # Redis url, enabled, prefix
    ├── database/              # Quản lý kết nối Database
    │   ├── prisma.module.ts   # Global Prisma Module
    │   └── prisma.service.ts  # Prisma Client lifecycle (connect/disconnect)
    ├── common/                # Tiện ích, Guards, Services dùng chung
    │   ├── decorators/        # Custom decorators (@CurrentUser)
    │   ├── guards/            # Guards xác thực (JwtAuthGuard, AdminGuard)
    │   ├── filters/           # Global exception filter (AllExceptionsFilter)
    │   ├── services/          # AiStatusMapper, TimeoutManager, InterruptResume, TokenUsage
    │   ├── streaming/         # StreamingManagerService (Server-Sent Events)
    │   └── kafka/             # Kafka constants, error codes (401-673), Base DTOs
    ├── messaging/             # Tầng Message Broker Kafka & In-memory Redis
    │   ├── messaging.module.ts            # Khởi tạo Kafka Client và Redis Client
    │   ├── kafka-producer.service.ts      # Producer validate 6 trường, gửi request Kafka
    │   ├── kafka-consumer.controller.ts   # Consumer lắng nghe 4 response topics
    │   ├── request-correlation.service.ts # Bộ điều phối ánh xạ Request-Response bất đồng bộ
    │   └── smoke-test.controller.ts       # Endpoint dev kiểm thử thông luồng Kafka
    └── modules/               # Các Domain Modules chính
        ├── auth/              # Xác thực và quản lý tài khoản người dùng
        │   ├── auth/          # Đăng nhập, Đăng ký, cấp phát JWT Bearer token
        │   └── users/         # CRUD người dùng, Profile cá nhân, Đổi mật khẩu
        ├── document-setup/    # Thiết lập tài liệu nghiên cứu ban đầu (DS-01 -> DS-06)
        │   ├── document/      # Quản lý Document entity & Admin documents
        │   ├── proposal/      # Đề xuất đề tài (Ideation: domains, subdomains, keywords, titles)
        │   ├── document-keyword/ # Quản lý từ khóa chính, từ khóa phụ, truy vấn tìm kiếm
        │   ├── reference/     # Tìm kiếm bài báo khoa học, Open Access, tải PDF
        │   ├── user-uploaded-document/ # Upload PDF người dùng & vector hóa Qdrant RAG
        │   └── outline/       # Cấu trúc đề cương và phân bổ số lượng từ mục tiêu
        ├── write-report/      # Soạn thảo báo cáo & Phân tích dữ liệu (WR-01 -> WR-05)
        │   ├── report/        # Nội dung markdown luận văn (SSE streaming), chuyên đề 1/2/3
        │   ├── dataset/       # Quản lý tập dữ liệu & phân tích thống kê biến số
        │   ├── dataset-variable/ # Khai báo biến nghiên cứu (Type, Role, Scale, Unit)
        │   ├── proposed-method/  # Phương pháp đề xuất, mô hình, giả thuyết nghiên cứu
        │   └── generated-slide/  # Sinh slide thuyết trình PowerPoint (.pptx)
        ├── chatbot/           # Trợ lý AI trao đổi học thuật (CB-01 -> CB-04)
        │   ├── chat-session/  # Quản lý phiên chat (outer, inner, writer)
        │   └── chat-message/  # Lịch sử tin nhắn, RAG chat & Writer Agent SSE streaming
        ├── enhancement/       # Tối ưu hóa & Nâng cấp văn bản (EN-01 -> EN-04)
        │   ├── enhancement-session/    # Phiên cải tiến theo đoạn văn bản chọn (ai_in_doc)
        │   └── enhancement-suggestion/ # Gợi ý sửa lỗi, viết lại, học thuật hóa, review diff
        └── jobs/              # Quản lý tiến trình tác vụ bất đồng bộ
            ├── dto/           # DTOs create, update, resume, status
            ├── job.controller.ts # API theo dõi & điều khiển Job
            └── job.service.ts    # Logic trạng thái (pending, processing, finished, suspended)
```
