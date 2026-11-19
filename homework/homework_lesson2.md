# BÀI TẬP VỀ NHÀ: BUỔI 02
## Xây Dựng Kafka Producer & Consumer Bằng Python

---

### 1. Mục tiêu
- Xây dựng lớp Kafka Producer có callback xác nhận và bộ phát sinh dữ liệu (Simulator).
- Áp dụng kiểm tra hợp lệ dữ liệu 2 tầng (Envelope validation & Schema validation).
- Xây dựng Consumer cơ chế At-Least-Once Delivery với Manual Commit và cơ chế cách ly tin nhắn lỗi (Quarantine).

---

### 2. Tài liệu tham khảo
- Hướng dẫn vận hành chi tiết: [docs/lesson2_runbook.md](../docs/lesson2_runbook.md)

---

### 3. Danh sách file cần hoàn thiện (Tìm các khối `# TODO`)
1. `src/common/serialization.py`:
   - Hàm `serialize_json()`: Mã hóa dữ liệu dict sang bytes UTF-8.
   - Hàm `deserialize_json()`: Giải mã bytes sang dict an toàn.
2. `src/common/validation.py`:
   - Hàm `validate_order_created_event()`: Kiểm tra 2 tầng envelope và schema.
   - Hàm `validate_product_viewed_event()`: Kiểm tra event xem sản phẩm.
3. `src/producer/order_producer.py`:
   - Delivery callback và hàm `produce()` gửi message lên Kafka kèm partition key.
4. `src/consumer/order_consumer.py`:
   - Vòng lặp `poll()`, xử lý message, manual commit offset và ghi nhận quarantine.

---

### 4. Lệnh kiểm thử tự nghiệm thu
Chạy lệnh kiểm thử sau trong môi trường ảo của bạn:
```bash
# 1. Chạy Unit Test
pytest tests/unit/test_common.py tests/unit/test_simulator.py -v

# 2. Chạy thử Producer bắn 20 sự kiện mẫu lên Kafka
python -m src.simulators.order_generator --max-events 20 --topic order-events
```
*Kỳ vọng:* Tất cả các test đều `PASSED`.

---

### 5. Tiêu chí nghiệm thu & Cách nộp bài
- **Hạn nộp**: Trước 19:00 của buổi học tiếp theo.
- **Cách nộp**: Commit code lên repo GitHub cá nhân của bạn và nộp link GitHub vào Form của lớp.

