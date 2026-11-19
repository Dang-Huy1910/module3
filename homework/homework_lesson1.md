# BÀI TẬP VỀ NHÀ: BUỔI 01
## Khởi Động Hạ Tầng Kafka KRaft & Hợp Đồng Dữ Liệu

---

### 1. Mục tiêu
- Khởi động thành công cụm Kafka KRaft bằng Docker Compose.
- Nắm vững cấu trúc Hợp đồng dữ liệu (Data Contract) định dạng JSON Schema.
- Sử dụng thành thạo Kafka Console CLI để gửi và nhận bản ghi có phân vùng (Partition) và Offset.

---

### 2. Tài liệu tham khảo
- Hướng dẫn vận hành chi tiết: [docs/lesson1_runbook.md](../docs/lesson1_runbook.md)
- File hợp đồng dữ liệu:
  - `contracts/order_created.schema.json`
  - `contracts/product_viewed.schema.json`

---

### 3. Yêu cầu thực hiện
1. **Khởi động Kafka KRaft**:
   ```bash
   docker compose up -d kafka init-topics
   ```
   Kiểm tra hai topic `order-events` và `customer-activity-events` đã được tạo thành công.

2. **Gửi bản ghi mẫu qua CLI**:
   Dùng `kafka-console-producer.sh` gửi bản ghi có Key `ORD-1001` vào topic `order-events` theo hướng dẫn tại Mục 2 trong Runbook.

3. **Đọc bản ghi qua CLI**:
   Dùng `kafka-console-consumer.sh` đọc lại bản ghi vừa gửi, yêu cầu hiển thị rõ **Key**, **Partition** và **Offset**.

---

### 4. Tiêu chí nghiệm thu & Cách nộp bài
- **Bằng chứng hoàn thành**:
  - Chụp ảnh màn hình Terminal hiển thị danh sách Topic (`kafka-topics.sh --list`).
  - Chụp ảnh màn hình Terminal đọc được bản ghi hiển thị rõ `Key`, `Partition`, `Offset`.
- **Hạn nộp**: Trước 19:00 của buổi học tiếp theo.
- **Cách nộp**: Điền link ảnh/báo cáo hoặc đẩy ảnh vào thư mục cá nhân và nộp vào Form của lớp.

