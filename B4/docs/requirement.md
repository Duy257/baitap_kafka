# Bài 4: Tối ưu Throughput với Batching & Compression

## Mục tiêu
Tăng hiệu suất Producer lên gấp nhiều lần.

## Yêu cầu
Gửi 1 triệu message vào topic.

- **Lần 1**: Không set `batch.size` và `linger.ms` (giá trị default — Sarama tự động batch ngầm ~1MB), đo thời gian.
- **Lần 2**: Set `batch.size=65536` (64KB), `linger.ms=20`, `compression.type=snappy`. Đo lại thời gian.
- **Báo cáo**: Ghi lại sự chênh lệch về throughput (messages/giây) và dung lượng network payload đã được nén.

## Mục tiêu bài học
- Hiểu rõ ảnh hưởng của batch size, linger.ms và compression đến hiệu suất Producer.
- So sánh throughput (số message/giây) và mức sử dụng băng thông mạng giữa cấu hình mặc định và cấu hình tối ưu.
- Thực hành đo lường, báo cáo số liệu và giải thích.

## Những điều học được từ bài tập

### a) Hiệu quả của batching
- **Giảm overhead mạng**: Mỗi request TCP đều có header và độ trễ khởi tạo. Gom nhiều message vào một request giúp tiết kiệm đáng kể.
- **Tận dụng bandwidth**: Đường truyền mạng hiệu quả hơn khi gửi khối dữ liệu lớn thay vì nhiều gói nhỏ.
- Cấu hình `batch.size` và `linger.ms` là đánh đổi giữa độ trễ và throughput: `linger.ms` càng cao, độ trễ từng message càng lớn nhưng throughput tổng càng cao.

### b) Vai trò của compression
- **Giảm dung lượng truyền tải**: Nén làm giảm số byte phải gửi qua mạng, cho phép truyền nhiều message hơn trong cùng băng thông.
- **Tăng throughput hiệu quả**: Kết hợp batching + nén giúp tối ưu cả số request lẫn kích thước mỗi request.
- **Đánh đổi CPU**: Nén tiêu tốn CPU phía producer và broker, nhưng thường lợi ích về mạng lớn hơn nhiều trong hệ thống phân tán.

### c) Thiết kế producer hiệu quả trong thực tế
- Luôn sử dụng AsyncProducer cho ứng dụng cần throughput cao.
- Cấu hình batch size phù hợp với message size trung bình và yêu cầu độ trễ.
- Sử dụng nén Snappy hoặc LZ4 (nhanh, tỉ lệ nén khá) cho dữ liệu text, Zstd cho tỉ lệ nén cao hơn nếu CPU dư dả.

### d) Đo lường và tối ưu liên tục
- Luôn chạy benchmark với dữ liệu thực tế để chọn tham số phù hợp.
- Kafka khuyến nghị producer gửi không đồng bộ, batch, và nén để đạt hiệu suất hàng triệu msg/s.

## Mở rộng (tùy chọn)
- Thử nghiệm các mức nén khác: gzip, lz4, zstd để so sánh tỉ lệ nén và tốc độ nén.
- Tăng số partition lên 10-20 và so sánh throughput khi producer có nhiều leader partition để gửi song song.
- Theo dõi metric của Sarama bằng cách gán `config.MetricRegistry` để lấy chính xác số byte đã gửi qua mạng (outgoing-byte-rate, request-size-avg).
