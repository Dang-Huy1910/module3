# Hướng Dẫn Vận Hành Lab: Buổi 5 — Data Lake & ClickHouse Data Warehouse

Học phần này bổ sung nhánh lưu trữ lịch sử (Historical Path) chạy song song và độc lập với nhánh thời gian thực (Hot Path):
`Kafka -> Raw Archiver -> S3 Object Storage (SeaweedFS) -> Warehouse Loader -> ClickHouse OLAP`

---

## 1. Khởi động Hạ tầng Storage & Warehouse

Chạy các service lưu trữ và ClickHouse với profile `storage`:
```bash
docker compose --profile storage up -d object-storage clickhouse archiver
docker compose ps
```
*Kỳ vọng:* `object-storage`, `clickhouse`, `de-archiver` đều chạy thành công.

Kiểm tra kết nối ClickHouse:
```bash
docker compose exec clickhouse clickhouse-client --user default --password analytics-dev --query "SELECT version()"
```

Kiểm tra Bucket trên S3:
```bash
docker compose --profile storage exec archiver python -m src.archiver.inspect_objects --list-buckets
```

---

## 2. Thực thi Pipeline Đẩy Dữ Liệu vào Data Lake

### Bước 1: Phát 7 sự kiện mẫu vào Kafka
```bash
docker compose run --rm publisher python -m src.simulators.publish_fixture --file fixtures/module6_buoi2_events.jsonl
```
*Kỳ vọng:* Đã phát 7 records (3 đơn hàng O1, O2, O3 và 4 lượt xem V1, V2, V3, V4).

### Bước 2: Kiểm tra các object JSON đã lưu trên Data Lake (S3)
Kiểm tra topic đơn hàng:
```bash
docker compose --profile storage exec archiver python -m src.archiver.inspect_objects --prefix raw/topic=lab6-order-events/ --max 20
```
Kiểm tra topic lượt xem:
```bash
docker compose --profile storage exec archiver python -m src.archiver.inspect_objects --prefix raw/topic=lab6-activity-events/ --max 20
```
*Kỳ vọng:* 7 file JSON xuất hiện với cấu trúc phân cấp:
`raw/topic=<topic>/dt=<date>/partition=<partition>/offset=<offset>.json`.

---

## 3. Tải Dữ Liệu vào Data Warehouse (ClickHouse)

### Bước 1: Chạy tiến trình nạp (Warehouse Loader)
```bash
docker compose --profile storage run --rm warehouse-loader python -m src.warehouse.load_events --prefix raw/
```
*Kỳ vọng:* Nạp 3 bản ghi vào `analytics.orders` và 4 bản ghi vào `analytics.product_views`.

### Bước 2: Truy vấn kiểm tra dữ liệu phân tích
```bash
docker compose exec clickhouse clickhouse-client --user default --password analytics-dev --query "SELECT count(), sum(total_amount) FROM analytics.orders FINAL"
```
*Kỳ vọng chuẩn:* `3, 549000` (549.000 VND).

```bash
docker compose exec clickhouse clickhouse-client --user default --password analytics-dev --query "SELECT count() FROM analytics.product_views FINAL"
```
*Kỳ vọng chuẩn:* `4`.

---

## 4. Thử nghiệm chống trùng lặp tại ClickHouse (ReplacingMergeTree)

Phát lại dữ liệu mẫu với cờ `--republish` (tạo ra các offset Kafka mới nhưng giữ nguyên `event_id` nghiệp vụ):
```bash
docker compose run --rm publisher python -m src.simulators.publish_fixture --file fixtures/module6_buoi2_events.jsonl --republish
```
Chạy lại Warehouse Loader:
```bash
docker compose --profile storage run --rm warehouse-loader python -m src.warehouse.load_events --prefix raw/
```
Chạy lại câu lệnh SQL:
```bash
docker compose exec clickhouse clickhouse-client --user default --password analytics-dev --query "SELECT count(), sum(total_amount) FROM analytics.orders FINAL"
```
*Kỳ vọng:* Dù số dòng vật lý trong bảng có thể tăng lên nhưng với từ khóa `FINAL` của `ReplacingMergeTree`, số liệu logic vẫn **chính xác tuyệt đối là 3 đơn hàng và 549.000 VND**.

**Lưu ý truy vấn:** `ReplacingMergeTree` không chống trùng ngay tại thời điểm `INSERT`. Khi tính `count()` hoặc `sum()` trên các bảng này, dùng `FINAL`; truy vấn trực tiếp không có `FINAL` có thể đếm trùng sau khi nạp lại prefix.
