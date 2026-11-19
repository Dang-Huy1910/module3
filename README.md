# BK Fintech E-Commerce Realtime Data Platform (Starter Repository)

Chào mừng bạn đến với kho mã nguồn thực hành của học phần **Realtime Data Engineering & Deployment (Module 3)**.

Đây là repository khung sườn (**Starter Code**) chứa sẵn cấu hình hạ tầng, hợp đồng dữ liệu (JSON Schema), bộ dữ liệu mẫu và các khung hàm gợi ý. Nhiệm vụ của bạn là hoàn thiện từng module theo chỉ dẫn chi tiết trong các tờ **Lab Handout** tương ứng với từng buổi học.

---

## 1. Hướng Dẫn Bắt Đầu & Quy Trình Nộp Bài

### Bước 1: Fork Repository về GitHub cá nhân
1. Bấm vào nút **Fork** ở góc trên bên phải của repository này trên GitHub để tạo một bản sao về tài khoản của bạn.
2. Clone repo cá nhân đó về máy tính của bạn:
   ```bash
   git clone https://github.com/<your-username>/bk-fintech-realtime-platform-starter.git
   cd bk-fintech-realtime-platform-starter
   ```

### Bước 2: Chuẩn bị môi trường Python & Docker
```bash
# Tạo môi trường ảo Python
python -m venv .venv

# Kích hoạt trên Windows PowerShell:
.\.venv\Scripts\Activate.ps1
# Kích hoạt trên macOS/Linux:
source .venv/bin/activate

# Cài đặt các thư viện cần thiết
pip install -r requirements.txt
```

### Bước 3: Làm bài Lab theo từng buổi
Mở các file tương ứng trong `src/` và hoàn thiện các khối `# TODO`:
- **Buổi 1:** Làm quen Kafka KRaft, CLI, Partition & Offset ([docs/lesson1_runbook.md](docs/lesson1_runbook.md)).
- **Buổi 2:** Hoàn thiện `src/common/serialization.py`, `src/common/validation.py`, `src/producer/order_producer.py`, `src/consumer/order_consumer.py` ([docs/lesson2_runbook.md](docs/lesson2_runbook.md)).
- **Buổi 3:** Hoàn thiện `src/analytics/windowing.py`, `src/analytics/analytics_store.py` và chạy Streamlit Dashboard ([docs/lesson3_runbook.md](docs/lesson3_runbook.md)).
- **Buổi 4:** Đóng gói Docker Compose ([docs/lesson4_runbook.md](docs/lesson4_runbook.md)).
- **Buổi 5:** Hoàn thiện `src/archiver/s3_keys.py`, `src/warehouse/load_events.py` ([docs/lesson5_runbook.md](docs/lesson5_runbook.md)).
- **Buổi 6:** Đo lường Lag và chạy kịch bản phục hồi sự cố ([docs/lesson6_runbook.md](docs/lesson6_runbook.md)).

### Bước 4: Kiểm thử & Nộp bài
- Chạy Unit Test sau khi hoàn thành TODO của cả 6 buổi:
  ```bash
  python -m pytest tests/unit -v
  ```
- Trong buổi 2 và 3, dùng lệnh kiểm thử theo từng buổi trong runbook tương ứng; toàn bộ bộ test chỉ xanh sau khi hoàn thành các TODO của buổi 5.
- Commit code và đẩy lên GitHub cá nhân của bạn:
  ```bash
  git add .
  git commit -m "feat: complete lab session <X>"
  git push origin main
  ```
- **Nộp bài:** Gửi đường dẫn GitHub Repo của bạn kèm ảnh chụp màn hình terminal chứng minh hoàn thành checklist cuối mỗi buổi Handout theo quy định của Giảng viên/TA.
