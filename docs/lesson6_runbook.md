# Hướng Dẫn Vận Hành Lab: Buổi 6 — Giám Sát Hệ Thống Dữ Liệu & Xử Lý Sự Cố (Observability & Incident Response)

Học phần này trang bị hệ sinh thái giám sát chuẩn công nghiệp cho toàn bộ nền tảng dữ liệu thời gian thực:
- **Prometheus** (Port 9090): Thu thập các chỉ số thời gian thực (throughput, latency, lag, status).
- **Grafana** (Port 3000): Bảng điều khiển trực quan 4 chỉ số vàng (Gold Signals) không cần cấu hình thủ công.
- **Kafka Lag Exporter** (Port 9110): Đo lường độ trễ tiêu thụ giữa broker và consumer group `realtime-analytics-v1`.
- **Kịch bản thực chiến**: Xử lý sự cố Consumer chết (`stop analytics`), quan sát Lag dồn ứ trên Grafana, khởi động lại (`start analytics`) và nghiệm thu bắt kịp dữ liệu bằng công cụ xác thực `verify_run`.

---

## 1. Khởi động Toàn bộ Cụm Dịch vụ & Giám sát

Khởi chạy cả stack lưu trữ và giám sát bằng lệnh:
```bash
docker compose --profile storage --profile monitoring up -d --build
docker compose ps
```
*Kỳ vọng:* Các container `prometheus`, `grafana`, `de-lag-exporter`, `de-analytics` đều hoạt động.

### Kiểm tra cú pháp Prometheus và Rules:
```bash
docker compose --profile monitoring exec prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose --profile monitoring exec prometheus promtool check rules /etc/prometheus/alert_rules.yml
```

---

## 2. Truy cập Giao diện Giám sát

- **Prometheus UI:** Mở trình duyệt tại `http://localhost:9090`.
  Kiểm tra các biểu thức PromQL chuẩn:
  ```promql
  up{job="analytics"}
  sum(bk_kafka_consumer_lag_records{group="realtime-analytics-v1"})
  sum(rate(bk_pipeline_events_total{service="analytics",result="accepted"}[1m]))
  ```
- **Grafana Dashboard:** Mở trình duyệt tại `http://localhost:3000` (User/Password: `admin` / `admin`).
  Dashboard **E-Commerce Pipeline Observability** đã được tự động nạp (provisioned) sẵn với 4 bảng chỉ số quan trọng.

---

## 3. Kịch bản Thử Nghiệm Sự Cố Thực Chiến (Incident Workflow)

### Bước 1: Phát luồng sự kiện cơ sở (Baseline)
```bash
docker compose run --rm publisher python -m src.simulators.order_generator --count 80 --rate 4 --seed 42 --run-id m6b3-baseline
```
Quan sát trên Grafana: Throughput tăng lên ~4 events/s, Lag ở mức 0, Service Health hiển thị màu xanh.

### Bước 2: Giả lập sự cố Consumer bị dừng đột ngột (Service Outage)
```bash
docker compose stop analytics
```
Lúc này Analytics Consumer bị dừng. Tiếp tục phát thêm 100 sự kiện vào Kafka:
```bash
docker compose run --rm publisher python -m src.simulators.order_generator --count 100 --rate 5 --seed 43 --run-id m6b3-incident
```
**Hiện tượng quan sát được trên Grafana:**
- Cột **Service Health (Analytics)** chuyển sang đỏ (`0`).
- **Kafka Consumer Lag** tăng vọt lên đúng **100 records** do sự kiện nằm đợi trong Kafka chưa được đọc.

### Bước 3: Phục hồi sự cố (Recovery)
Khởi động lại Analytics Consumer:
```bash
docker compose start analytics
docker compose logs --tail 100 analytics
```
**Hiện tượng quan sát được:**
- Consumer tiếp tục đọc từ offset đã commit trước đó.
- Cột **Kafka Consumer Lag** trên Grafana giảm dần về **0** khi Consumer tiêu thụ hết lượng tin tồn đọng.

### Bước 4: Nghiệm thu tính toàn vẹn dữ liệu (Verification)
Chạy script kiểm tra thực tế trong database SQLite:
```bash
docker compose exec analytics python -m src.monitoring.verify_run --run-id m6b3-incident --expected 100
```
*Kỳ vọng:* Trả về **`STATUS: PASSED`** xác nhận đúng 100 sự kiện của đợt sự cố đã được xử lý và ghi nhận đầy đủ, không thiếu sót và không bị duplicate logic!
