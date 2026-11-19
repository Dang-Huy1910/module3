# Hướng Dẫn Vận Hành Lab: Buổi 4 — Container Hóa Hệ Thống Dữ Liệu Bằng Docker Compose

Học phần này đóng gói toàn bộ nền tảng E-commerce Streaming thành một cụm các container hoàn chỉnh, cô lập và tái lập được trên mọi máy tính cá nhân.

---

## 1. Kiến trúc hệ sinh thái Container

```text
[ Simulator (demo) ] ──(kafka:29092)──> [ Kafka Broker (KRaft) ]
                                                │
                                                ▼ (kafka:29092)
                                       [ Analytics Consumer ]
                                                │ (Ghi SQLite)
                                                ▼
                                    [ Volume: analytics-data ]
                                                ▲
                                                │ (Đọc :ro)
                                       [ Streamlit Dashboard ] ──> http://localhost:8501
```

---

## 2. Các lệnh kiểm thử & Vận hành chính thức

### Bước 1: Kiểm tra cú pháp Compose
```bash
docker compose config
```
*Kỳ vọng:* Cấu hình hợp lệ, không có lỗi parsing YAML hoặc volume.

### Bước 2: Build các image ứng dụng
```bash
docker compose build analytics dashboard
```
*Kỳ vọng:* Image được build qua 2 tầng (builder + runner), phân quyền non-root `appuser`.

### Bước 3: Khởi chạy toàn bộ cụm dịch vụ
```bash
docker compose up -d --build
```
Kiểm tra trạng thái các container:
```bash
docker compose ps
```
*Kỳ vọng:*
- `kafka`: `healthy`
- `kafka-init-topics`: `exited (0)` (dịch vụ khởi tạo 1 lần thành công)
- `de-analytics`: `running`
- `de-dashboard`: `running`

### Bước 4: Xem log hoạt động của Analytics Consumer
```bash
docker compose logs --tail 40 -f analytics
```

### Bước 5: Kích hoạt Simulator trong container (Profile Demo)
```bash
docker compose --profile demo run --rm order-simulator python -m src.simulators.order_generator --count 100 --rate 5 --seed 42 --run-id m6-demo --bootstrap-servers kafka:29092
```

### Bước 6: Truy cập giao diện trực quan
Mở trình duyệt: `http://localhost:8501`.
Quan sát các thẻ KPI và biểu đồ tự động cập nhật khi sự kiện từ container simulator chạy.

---

## 3. Kiểm thử tính bền vững của dữ liệu (Volume Persistence)

Dừng và khởi động lại cụm container mà **KHÔNG** xóa volume:
```bash
docker compose down
docker compose up -d
```
Mở lại `http://localhost:8501`, toàn bộ số liệu đơn hàng và doanh thu đã tính toán trước đó vẫn được bảo toàn nguyên vẹn nhờ Docker named volume `bk_fintech_analytics_data`.
