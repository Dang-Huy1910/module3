# BÀI TẬP VỀ NHÀ: BUỔI 06
## Giám Sát Hệ Thống (Observability) & Xử Lý Sự Cố

---

### 1. Mục tiêu
- Thiết lập hệ sinh thái giám sát toàn diện với **Prometheus** và **Grafana**.
- Theo dõi các chỉ số quan trọng (Metrics): Consumer Group Lag, Tỷ lệ lỗi (Quarantine rate), Throughput.
- Chạy kịch bản kiểm thử toàn chu trình (End-to-End Verification) để xác thực độ tin cậy của toàn bộ nền tảng.

---

### 2. Tài liệu tham khảo
- Hướng dẫn vận hành chi tiết: [docs/lesson6_runbook.md](../docs/lesson6_runbook.md)

---

### 3. Yêu cầu thực hiện
1. **Khởi động dịch vụ giám sát**:
   ```bash
   docker compose --profile monitoring up -d prometheus grafana lag-exporter
   ```
2. **Quan sát Dashboard & Metric**:
   - Truy cập Prometheus UI: `http://localhost:9090`
   - Truy cập Grafana Dashboard: `http://localhost:3000` (User/Password: `admin`/`admin`).
   - Kiểm tra các biểu đồ: Consumer Lag, Throughput, Dead Letter Quarantine.
3. **Chạy kịch bản nghiệm thu toàn chu trình (E2E Verification)**:
   ```bash
   python -m src.monitoring.verify_run
   ```

---

### 4. Tiêu chí nghiệm thu & Cách nộp bài
- **Yêu cầu đạt**: Script `verify_run.py` hoàn thành với thông báo tất cả các bước (Broker, Consumers, Data Lake, ClickHouse, Lag Exporter) đều báo `[PASS]`.
- **Hạn nộp**: Trước 19:00 của buổi học tiếp theo.
- **Cách nộp**: Chụp ảnh màn hình Dashboard Grafana và nộp toàn bộ link repo cá nhân hoàn thiện vào Form tổng kết khóa học.

