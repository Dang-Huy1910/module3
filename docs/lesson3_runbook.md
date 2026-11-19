# Hướng Dẫn Vận Hành Lab: Buổi 3 — Streaming Analytics & Dashboard

Tài liệu hướng dẫn chạy và nghiệm thu pipeline xử lý dữ liệu luồng đa chủ đề (multi-topic: `order-events` và `customer-activity-events`), tổng hợp chỉ số thời gian thực theo khung 1 phút (1-minute window) lưu vào SQLite (chế độ WAL) và hiển thị lên giao diện Streamlit Dashboard.

---

## 1. Chuẩn bị môi trường & Topic Lab

1. Khởi động Kafka và các topic lab:
   ```bash
   docker compose up -d
   ```
2. Khởi tạo cơ sở dữ liệu SQLite:
   ```bash
   python -m src.analytics.init_db --db data/realtime_analytics_lab3.db
   ```

---

## 2. Kiểm thử Unit Tests
```bash
python -m pytest tests/unit/test_common.py tests/unit/test_simulator.py tests/unit/test_windowing.py tests/unit/test_analytics_store.py -v
```

---

## 3. Khởi chạy Pipeline và Dashboard

### Bước 1: Khởi chạy Streamlit Dashboard (Terminal 1)
```bash
streamlit run src/dashboard/app.py -- --db data/realtime_analytics_lab3.db
```
Mở trình duyệt tại: `http://localhost:8501`. Dashboard sẽ tự động cập nhật mỗi 5 giây.

### Bước 2: Khởi chạy Analytics Consumer (Terminal 2)
```bash
python -m src.analytics.analytics_consumer --bootstrap localhost:9092 --order-topic lab3-order-events --activity-topic lab3-activity-events --group lab3-analytics-v1 --db data/realtime_analytics_lab3.db
```

### Bước 3: Phát dữ liệu kiểm thử (Terminal 3)

1. **Phát bộ dữ liệu cơ sở (Base events):**
   ```bash
   python -m src.simulators.fixture_publisher --fixture fixtures/events/lesson3_base.jsonl --bootstrap localhost:9092 --order-topic lab3-order-events --activity-topic lab3-activity-events
   ```
   Kiểm tra số liệu:
   ```bash
   python -m src.analytics.inspect_db --db data/realtime_analytics_lab3.db --table dashboard_minute
   ```
   *Kỳ vọng:* Cửa sổ `10:00:00Z` có 2 đơn hàng (399.000 đ), 2 lượt xem, 2 khách hàng hoạt động; cửa sổ `10:01:00Z` có 1 đơn hàng (150.000 đ), 1 lượt xem, 1 khách hàng.

2. **Thử nghiệm chống trùng lặp (Duplicate test):**
   ```bash
   python -m src.simulators.fixture_publisher --fixture fixtures/events/lesson3_duplicate.jsonl --bootstrap localhost:9092 --order-topic lab3-order-events --activity-topic lab3-activity-events
   ```
   *Kỳ vọng:* Số liệu không thay đổi do cơ chế Deduplication tại SQLite.

3. **Thử nghiệm sự kiện đến muộn (Late arriving event):**
   ```bash
   python -m src.simulators.fixture_publisher --fixture fixtures/events/lesson3_late.jsonl --bootstrap localhost:9092 --order-topic lab3-order-events --activity-topic lab3-activity-events
   ```
   *Kỳ vọng:* Cửa sổ `10:00:00Z` cập nhật thêm 1 lượt xem (thành 3 lượt xem, 3 khách hàng).

4. **Thử nghiệm cách ly bản ghi lỗi (Invalid test):**
   ```bash
   python -m src.simulators.fixture_publisher --fixture fixtures/events/lesson3_invalid.jsonl --bootstrap localhost:9092 --order-topic lab3-order-events --activity-topic lab3-activity-events
   ```
   *Kỳ vọng:* Bản ghi lỗi được lưu vào bảng `rejected_events`, không ảnh hưởng số liệu thống kê.
