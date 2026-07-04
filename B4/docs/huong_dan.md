# Hướng dẫn Kafka

## Chuẩn bị môi trường

- Kafka cluster (local, ít nhất 1 broker).
- Topic `throughput_test` với nhiều partition (đề xuất 3 partition để tận dụng song song).

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic throughput_test \
  --partitions 3 --replication-factor 1
```

- Golang 1.21+ và thư viện Sarama:

```bash
go get github.com/IBM/sarama
```

## Phân tích cấu hình Sarama liên quan

| Tham số Kafka                           | Cấu hình Sarama                | Ý nghĩa                                                        |
| --------------------------------------- | ------------------------------ | -------------------------------------------------------------- |
| `batch.size`                            | `Producer.Flush.Bytes`         | Kích thước batch tối đa (byte), đủ thì flush ngay              |
| `linger.ms`                             | `Producer.Flush.Frequency`     | Thời gian chờ tối đa trước khi flush batch (nếu chưa đủ Bytes) |
| `compression.type`                      | `Producer.Compression`         | Thuật toán nén (none, snappy, gzip, lz4, zstd)                 |
| `max.in.flight.requests.per.connection` | `Producer.MaxInFlightRequests` | Ảnh hưởng thứ tự, không liên quan trực tiếp batching           |

**Lưu ý:** Sarama sử dụng `AsyncProducer` mới phát huy hiệu quả batching. `SyncProducer` sẽ flush ngay từng message, không tận dụng được batch.

## CLI flags

| Flag           | Mặc định          | Mô tả                                            |
| -------------- | ----------------- | ------------------------------------------------ |
| `-broker`      | `localhost:9092`  | Địa chỉ Kafka broker                             |
| `-topic`       | `throughput_test` | Tên topic                                        |
| `-messages`    | `1000000`         | Số message cần gửi                               |
| `-batch-size`  | `0`               | Kích thước batch (byte), 0 = không giới hạn byte |
| `-linger`      | `0`               | Thời gian chờ (ms) trước khi flush, 0 = gửi ngay |
| `-compression` | `none`            | Thuật toán nén: none, snappy, gzip, lz4, zstd    |

## Cách chạy và thu thập kết quả

### Lần 1: Cấu hình mặc định của Sarama

```bash
go run main.go
```

**Lưu ý:** Khi `batch-size=0` và `linger=0`, Sarama vẫn tự động batch message theo ngưỡng `MaxMessageBytes` (~1MB). Đây không phải "gửi từng message một" mà là batch ngầm định — vẫn cho thấy hiệu quả khi tăng batch chủ động.

### Lần 2: Cấu hình tối ưu (batch 64KB, chờ 20ms, nén Snappy)

```bash
go run main.go -batch-size=65536 -linger=20 -compression=snappy
```

### Tuỳ chỉnh khác

```bash
# Gửi 500k message tới topic khác, broker khác
go run main.go -broker=broker1:9092 -topic=my_topic -messages=500000

# Dùng nén Zstd
go run main.go -batch-size=65536 -linger=20 -compression=zstd
```

### Dừng giữa chừng

Nhấn `Ctrl+C` để dừng benchmark an toàn — producer sẽ flush các message đã gửi và in kết quả partial.

**Mẹo:** Tăng số partition của topic lên 6 hoặc nhiều hơn để thấy rõ sự khác biệt, vì nhiều partition giúp tận dụng batch song song.

## Báo cáo mẫu và phân tích

Kết quả thực nghiệm (ví dụ trên máy local):

| Chỉ số                        | Lần 1 (mặc định)       | Lần 2 (batch+snappy) |
| ----------------------------- | ---------------------- | -------------------- |
| Thời gian (giây)              | 62.5                   | 8.3                  |
| Throughput (msg/s)            | ~16,000                | ~120,000             |
| Dữ liệu thô (MB)              | 55 MB                  | 55 MB                |
| Dung lượng mạng thực tế (ước) | ~55 MB (mỗi msg 1 req) | ~18 MB (nén + gộp)   |

### Nhận xét

- Throughput tăng ~7.5 lần nhờ batching giảm số lần gửi request xuống còn 1/50 (batch 64KB chứa ~1200 message 55 byte). Mỗi request chỉ tốn một lần round-trip mạng.
- `Linger.ms=20` cho phép gom message trong 20ms, đảm bảo batch đủ lớn ngay cả khi tốc độ gửi chưa lấp đầy 64KB ngay lập tức.
- Nén Snappy giảm kích thước payload (thường đạt tỉ lệ 2-3x với văn bản), giảm băng thông mạng và thời gian truyền, góp phần tăng throughput.
- Số request/giây giảm mạnh → giảm áp lực lên broker, cho phép producer đẩy được nhiều message hơn trong cùng đơn vị thời gian.
