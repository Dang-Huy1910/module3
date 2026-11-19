# Hướng Dẫn Vận Hành Lab: Buổi 1 — Khởi Động Hạ Tầng Kafka KRaft & Hợp Đồng Dữ Liệu

Buổi đầu tiên giúp học viên tiếp cận kiến trúc Streaming Broker với **Apache Kafka ở chế độ KRaft** (không phụ thuộc Zookeeper), tạo Topic, và sử dụng bộ công cụ Kafka CLI để trực tiếp quan sát Partition, Key, Offset và Consumer Group.

---

## 1. Khởi động Kafka Broker

1. Di chuyển vào thư mục gốc repository:
   ```bash
   docker compose up -d kafka init-topics
   ```
2. Kiểm tra container hoạt động:
   ```bash
   docker compose ps
   ```
   *Kỳ vọng:* `kafka` ở trạng thái running (healthy), `kafka-init-topics` chạy xong và thoát mã 0.

3. Kiểm tra danh sách các Topic đã được khởi tạo:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
   ```
   *Kỳ vọng:* Xuất hiện `order-events` và `customer-activity-events`.

---

## 2. Thao tác với Kafka CLI (Theo Lab Handout)

### Bước 1: Gửi bản ghi JSON có Key vào topic `order-events`
Mở Terminal 1 và kích hoạt Console Producer:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic order-events --property parse.key=true --property "key.separator=|"
```
Nhập lần lượt các bản ghi từ file `fixtures/events/valid_order.json`:
```text
ORD-1001|{"event_id":"evt-order-001","event_type":"OrderCreated","event_version":1,"event_time":"2026-11-02T10:00:05Z","source":"order-simulator","order_id":"ORD-1001","customer_id":"CUS-001","items":[{"product_id":"P-101","quantity":2,"unit_price":150000}],"total_amount":300000,"currency":"VND"}
```

### Bước 2: Đọc dữ liệu và hiển thị Partition / Offset
Mở Terminal 2 và kích hoạt Console Consumer:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic order-events --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```
*Kỳ vọng:* Màn hình hiển thị đầy đủ Key, Partition và số Offset tương ứng của bản ghi.
