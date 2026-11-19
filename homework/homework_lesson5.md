# BÀI TẬP VỀ NHÀ: BUỔI 05
## Data Lake (S3) & Data Warehouse (ClickHouse)

---

### 1. Mục tiêu
- Xây dựng tầng lưu trữ Data Lake dựa trên chuẩn S3 (SeaweedFS) với định dạng đường dẫn phân vùng (Partitioning) chuẩn theo ngày/giờ.
- Tích hợp và cấu hình cơ sở dữ liệu OLAP ClickHouse với Engine `ReplacingMergeTree`.
- Xây dựng Pipeline nạp dữ liệu hàng loạt (Batch loader) từ Data Lake vào ClickHouse.

---

### 2. Tài liệu tham khảo
- Hướng dẫn vận hành chi tiết: [docs/lesson5_runbook.md](../docs/lesson5_runbook.md)

---

### 3. Danh sách file cần hoàn thiện (Tìm các khối `# TODO`)
1. `src/archiver/s3_keys.py`:
   - Hàm `generate_raw_s3_key()`: Tạo S3 key xác định theo mẫu:  
     `raw/<event_type>/year=YYYY/month=MM/day=DD/hour=HH/<event_id>.json`
2. `src/warehouse/load_events.py`:
   - Hàm `load_raw_events_to_clickhouse()`: Quét danh sách file từ S3 bucket `data-lake`, phân tích JSON và nạp bản ghi vào bảng `analytics.order_events_raw`.

---

### 4. Lệnh kiểm thử tự nghiệm thu
Chạy lệnh kiểm thử sau trên máy:
```bash
# 1. Khởi động dịch vụ S3 và ClickHouse
docker compose up -d object-storage clickhouse

# 2. Chạy Unit Test kiểm tra S3 Key và Warehouse Loader
pytest tests/unit/test_s3_keys.py tests/unit/test_warehouse_loader.py -v

# 3. Chạy script nạp dữ liệu từ S3 sang ClickHouse
python -m src.warehouse.load_events

# 4. Kiểm tra số lượng bản ghi trong ClickHouse
docker compose exec clickhouse clickhouse-client --query "SELECT count(*) FROM analytics.order_events_raw;"
```
*Kỳ vọng:* Tất cả unit test `PASSED` và truy vấn ClickHouse trả về số lượng bản ghi tương ứng.

---

### 5. Tiêu chí nghiệm thu & Cách nộp bài
- **Hạn nộp**: Trước 19:00 của buổi học tiếp theo.
- **Cách nộp**: Commit code lên repo GitHub cá nhân của bạn và nộp link GitHub vào Form của lớp.

