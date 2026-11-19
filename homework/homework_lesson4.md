# BÀI TẬP VỀ NHÀ: BUỔI 04
## Container Hóa Hệ Thống Dữ Liệu Bằng Docker Compose

---

### 1. Mục tiêu
- Đóng gói toàn bộ nền tảng ứng dụng Python bằng Docker Image tối ưu (Multi-stage build).
- Thiết lập bảo mật container với quyền người dùng không đặc quyền (`appuser`).
- Vận hành toàn bộ hệ sinh thái xử lý dữ liệu tự động bằng Docker Compose.

---

### 2. Tài liệu tham khảo
- Hướng dẫn vận hành chi tiết: [docs/lesson4_runbook.md](../docs/lesson4_runbook.md)

---

### 3. Yêu cầu thực hiện
1. **Kiểm tra và hoàn thiện `Dockerfile`**:
   - Giai đoạn `builder`: Cài đặt thư viện dependencies cần thiết.
   - Giai đoạn `runtime`: Kế thừa python:3.12-slim, tạo người dùng `appuser`, cấp quyền thư mục `/data`.
2. **Cấu hình `compose.yaml`**:
   - Đảm bảo các service (`order-consumer`, `analytics-consumer`, `dashboard`) kết nối đúng mạng `data-network`.
   - Mount đúng volume `analytics-data` vào `/data`.

---

### 4. Lệnh kiểm thử tự nghiệm thu
Chạy lệnh kiểm thử sau trên máy:
```bash
# 1. Build image Docker
docker compose build

# 2. Khởi động toàn bộ nền tảng service ngầm
docker compose up -d

# 3. Kiểm tra trạng thái các container
docker compose ps

# 4. Xem log hoạt động của analytics-consumer
docker compose logs -f analytics-consumer
```
*Kỳ vọng:* Mở trình duyệt tại `http://localhost:8501`, giao diện Dashboard hiển thị đầy đủ và tự động cập nhật dữ liệu khi có bản ghi mới.

---

### 5. Tiêu chí nghiệm thu & Cách nộp bài
- **Hạn nộp**: Trước 19:00 của buổi học tiếp theo.
- **Cách nộp**: Commit file cấu hình lên repo GitHub cá nhân của bạn và nộp link GitHub vào Form của lớp.

