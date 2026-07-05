# Bài 6: Xử lý Offset thủ công (Manual Commit) và Exactly-Once trong Consumer

## Mục tiêu
Tránh mất mát dữ liệu khi xử lý bên ngoài Kafka (ví dụ gọi API, ghi DB).

## Yêu cầu

- Producer gửi 100 message đánh số thứ tự (1 đến 100).
- Consumer xử lý message và giả lập ghi vào Database. Tuy nhiên, message số chẵn sẽ gây ra Exception (giả lập DB fail).
- Cơ chế: Consumer phải commit offset **sau khi** xử lý thành công. Nếu message số chẵn fail, **không được** commit offset đó. Tắt auto commit (`enable.auto.commit=false`).
- Khởi động lại consumer và đảm bảo nó xử lý lại đúng các message số lẻ và số chẵn (lặp cho đến khi thành công).

## Mục tiêu bài học

- Hiểu rõ cơ chế commit offset thủ công để kiểm soát chính xác thời điểm đánh dấu đã xử lý thành công.
- Biết cách cấu hình consumer tắt auto-commit và tự quyết định commit offset sau khi nghiệp vụ thành công.
- Xây dựng consumer có khả năng retry khi xử lý thất bại mà không bị mất dữ liệu.
- Nắm được nguyên lý Exactly-Once Semantics phía consumer (kết hợp với xử lý idempotent bên ngoài).

## Mô tả bài toán

- Producer gửi 100 message vào topic `orders`, nội dung đánh số từ 1 đến 100.
- Consumer đọc message, mô phỏng ghi vào database:
  - Message có số lẻ → ghi thành công.
  - Message có số chẵn → giả lập lỗi DB (để gây ra tình huống retry).
- Consumer chỉ được commit offset khi xử lý thành công. Nếu thất bại, không commit.
- Khi gặp lỗi, consumer sẽ tự động thử lại cho đến khi thành công (tối đa vài lần) rồi mới chuyển sang message tiếp theo.
- Tắt chế độ auto-commit của Kafka consumer.
