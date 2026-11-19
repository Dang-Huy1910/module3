# BÀI TẬP VỀ NHÀ: BUỔI 03
## Streaming Analytics & Dashboard Streamlit

---

### 1. Mục tiêu
- Cắt cửa sổ thời gian dạng Tumbling Window 1 phút theo chuẩn UTC từ `event_time`.
- Xử lý trùng lặp dữ liệu (Deduplication) dựa trên `event_id`.
- Quản lý giao dịch nguyên tử (Atomic transaction) trên SQLite (chế độ WAL).
- Trực quan hóa dữ liệu chỉ số GMV và số đơn hàng theo thời gian thực trên Streamlit Dashboard.

---

### 2. Tài liệu tham khảo
- Hướng dẫn vận hành chi tiết: [docs/lesson3_runbook.md](../docs/lesson3_runbook.md)

---

### 3. Danh sách file cần hoàn thiện (Tìm các khối `# TODO`)
1. `src/analytics/windowing.py`:
   - Hàm `get_tumbling_window_start()`: Phân tích `event_time` (ISO 8601 UTC) và trả về mốc bắt đầu của cửa sổ 1 phút (ví dụ: `2026-11-02T10:00:00Z`).
2. `src/analytics/analytics_store.py`:
   - Hàm `record_event()`: Mở transaction SQLite, kiểm tra tính duy nhất của `event_id`, cộng dồn GMV và số đơn hàng vào bảng `hourly_metrics`.

---

### 4. Lệnh kiểm thử tự nghiệm thu
Chạy lệnh kiểm thử sau trong môi trường ảo của bạn:
```bash
# 1. Chạy Unit Test kiểm tra cắt cửa sổ và tính toán GMV
pytest tests/unit/test_windowing.py tests/unit/test_analytics_store.py -v

# 2. Bắn kịch bản dữ liệu mẫu (chứa cả duplicate và invalid event)
python -m src.simulators.fixture_publisher --fixture fixtures/scenarios/analytics_sample.json

# 3. Mở Dashboard Streamlit kiểm tra trực quan
streamlit run src/dashboard/app.py
```
*Kỳ vọng:* GMV hiển thị chính xác đúng giá trị sau khi đã trừ đi các event duplicate và quarantine.

---

### 5. Tiêu chí nghiệm thu & Cách nộp bài
- **Hạn nộp**: Trước 19:00 của buổi học tiếp theo.
- **Cách nộp**: Commit code lên repo GitHub cá nhân của bạn và nộp link GitHub vào Form của lớp.

