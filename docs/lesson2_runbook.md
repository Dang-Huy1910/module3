# Hướng Dẫn Vận Hành Lab: Buổi 2 — Xây Dựng Kafka Producer & Consumer Bằng Python

Tài liệu này hướng dẫn chạy thử nghiệm luồng sinh dữ liệu bằng Python và ghi nhận sự kiện qua Kafka Consumer với cơ chế commit thủ công và phân loại cách ly lỗi (quarantine).

---

## 1. Chuẩn bị môi trường

Khởi động hạ tầng Kafka (từ Buổi 1):
```bash
docker compose up -d
docker compose ps
```

Kích hoạt môi trường Python ảo và cài đặt thư viện:
```bash
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

---

## 2. Kiểm thử Unit Tests
Đảm bảo các logic hợp đồng dữ liệu và chuẩn hóa hoạt động chính xác:
```bash
python -m pytest tests/unit/test_common.py tests/unit/test_simulator.py -v
```
Các test của buổi 3 và buổi 5 vẫn còn TODO nên chưa nằm trong bước nghiệm thu này.

---

## 3. Thực thi Pipeline

### Bước 1: Khởi chạy Consumer (Terminal 1)
Mở một cửa sổ terminal riêng và chạy Consumer:
```bash
python -m src.consumer.order_consumer --group lab2-order-v1 --output-dir data
```
Consumer sẽ lắng nghe tại topic `order-events` và ghi các bản tin hợp lệ vào `data/processed.jsonl`.

### Bước 2: Khởi chạy Simulator (Terminal 2)
Mở một cửa sổ terminal thứ 2 và phát 100 sự kiện đơn hàng:
```bash
python -m src.simulators.order_generator --count 100 --rate 5 --seed 42 --run-id lab2-a
```
Quan sát thông báo:
- Producer gửi thành công 100 sự kiện với xác nhận từ Kafka broker (`delivered=100`, `failed=0`).
- Bên Terminal 1, Consumer log ra lần lượt các dòng:
  - `[PERSISTED]` khi bản ghi đã ghi xuống đĩa thành công và gọi `fsync`.
  - `[OFFSET_COMMITTED]` ngay sau đó.

---

## 4. Thử nghiệm kịch bản giả lập sự cố (Crash before commit)

### Bước 1: Khởi chạy Consumer với cờ dừng đột ngột
```bash
python -m src.consumer.order_consumer --group lab2-crash-v1 --output-dir data/crash-test --crash-after-write-event-id lab2-crash-order-0001
```

### Bước 2: Sinh dữ liệu kiểm thử
Tạo sự kiện có ID trùng với cờ crash:
```bash
# Sinh dữ liệu và quan sát Consumer bị exit code 42
```

### Bước 3: Chạy lại Consumer cùng group ID mà không có cờ crash
```bash
python -m src.consumer.order_consumer --group lab2-crash-v1 --output-dir data/crash-test
```
**Kết quả quan sát được:**
Vì offset chưa được commit trước khi tiến trình bị dừng, khi khởi động lại, Consumer sẽ đọc lại chính sự kiện đó và ghi nhận lại. Đây là minh chứng rõ ràng cho ngữ nghĩa chuyển phát **At-least-once**.
