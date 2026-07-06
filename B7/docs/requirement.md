# Bài 8: Idempotent Producer & Exactly Once Semantics (EOS)

## 1. Mục tiêu bài học

- Hiểu rõ cơ chế Idempotent Producer trong Kafka.
- Cấu hình Producer với `enable.idempotence=true` và `acks=all`.
- Mô phỏng lỗi mạng khiến Producer retry, chứng minh rằng broker chỉ ghi nhận 1 message dù Producer gửi lại nhiều lần.
- Kiểm tra thực tế qua Consumer hoặc log segment.

## 2. Lý thuyết nền tảng

- **Producer ID (PID):** Kafka broker tự sinh cho mỗi producer idempotent khi khởi tạo.
- **Sequence Number:** Mỗi message gửi đến một partition được gán số thứ tự tăng dần.
- Broker duy trì 5 sequence number gần nhất cho mỗi (PID, partition). Nếu nhận message có sequence number trùng hoặc nhỏ hơn → loại bỏ trùng lặp.
- Khi Producer retry do mất ack, nó dùng lại sequence number cũ → broker không ghi thêm.
- Cấu hình tối thiểu: `enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection ≤ 5`.

## 3. Chuẩn bị môi trường

- Kafka cluster (local).
- Topic `financial` với 1 partition (để dễ quan sát).

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic financial --partitions 1 --replication-factor 1
```

- Cài đặt Sarama:

```bash
go get github.com/IBM/sarama
```

## 4. Code Producer (Idempotent + Retry nội bộ)

File `producer.go`:

```go
package main

import (
    "fmt"
    "log"
    "os"
    "os/signal"
    "time"

    "github.com/IBM/sarama"
)

func main() {
    config := sarama.NewConfig()
    // Bắt buộc để idempotent
    config.Producer.Idempotent = true
    config.Producer.RequiredAcks = sarama.WaitForAll
    // Đảm bảo thứ tự khi retry (idempotent yêu cầu ≤5)
    config.Producer.MaxInFlightRequests = 1
    // Số lần retry tối đa khi gặp lỗi retriable
    config.Producer.Retry.Max = 10
    config.Producer.Retry.Backoff = 100 * time.Millisecond

    producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
    if err != nil {
        log.Fatalf("Tạo producer thất bại: %v", err)
    }
    defer producer.AsyncClose()

    // Xử lý kết quả
    go func() {
        for msg := range producer.Successes() {
            fmt.Printf("✅ Message gửi thành công: offset=%d, partition=%d\n", msg.Offset, msg.Partition)
        }
    }()
    go func() {
        for err := range producer.Errors() {
            fmt.Printf("❌ Lỗi: %v\n", err)
        }
    }()

    topic := "financial"
    msg := &sarama.ProducerMessage{
        Topic: topic,
        Key:   sarama.StringEncoder("order_123"),   // key để đảm bảo vào cùng partition
        Value: sarama.StringEncoder(`{"order_id": 123}`),
    }

    fmt.Println("Gửi message lần đầu...")
    producer.Input() <- msg

    // Giữ chương trình chạy để có thời gian mô phỏng lỗi
    fmt.Println("Nhấn Ctrl+C để dừng.")
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, os.Interrupt)
    <-sig
    fmt.Println("Đã dừng producer.")
}
```

**Giải thích:**
- `Idempotent = true` tự động thiết lập `MaxInFlightRequests = 5` nếu không set. Ta set lại = 1 để dễ kiểm soát.
- Producer gửi message, sau đó chờ tín hiệu dừng để có thời gian mô phỏng lỗi.

## 5. Mô phỏng lỗi mạng (network timeout) để kích hoạt retry

Để Producer thực hiện retry với cùng sequence number, ta cần tạo tình huống gói tin ack từ broker về producer bị mất, dẫn đến timeout.

### Cách 1: Dùng iptables (Linux)

Chạy producer. Ngay khi thấy log "Gửi message lần đầu...", chạy lệnh sau để chặn gói tin từ broker về producer (port 9092):

```bash
sudo iptables -A OUTPUT -p tcp --sport 9092 -j DROP
```

Lệnh này chặn các gói tin đi ra từ máy (tức là từ broker nếu broker cùng máy) có source port 9092. Nếu broker ở máy khác, cần điều chỉnh.

Producer sẽ không nhận được ack, retry vài lần (in ra lỗi).

Sau khoảng 5 giây, bỏ chặn:

```bash
sudo iptables -D OUTPUT -p tcp --sport 9092 -j DROP
```

Producer sẽ nhận được ack từ lần retry cuối cùng và in "✅ Message gửi thành công".

### Cách 2: Dùng toxiproxy (đa nền tảng)

Cài toxiproxy: https://github.com/Shopify/toxiproxy

Tạo proxy:

```bash
toxiproxy-cli create kafka -l localhost:9093 -u localhost:9092
toxiproxy-cli toxic add kafka -t timeout -a timeout=2000
```

Đổi producer kết nối đến `localhost:9093`. Khi gửi message, proxy sẽ tạo timeout. Producer sẽ retry. Sau đó tắt timeout, producer thành công.

> Chọn cách thuận tiện. Trong hướng dẫn này, tôi dùng iptables.

## 6. Consumer kiểm tra (chỉ thấy 1 message)

File `consumer.go`:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"

    "github.com/IBM/sarama"
)

func main() {
    config := sarama.NewConfig()
    config.Consumer.Offsets.Initial = sarama.OffsetOldest
    consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
    if err != nil {
        log.Fatalf("Tạo consumer lỗi: %v", err)
    }
    defer consumer.Close()

    topic := "financial"
    partition, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
    if err != nil {
        log.Fatalf("Lỗi consume partition: %v", err)
    }
    defer partition.Close()

    ctx, cancel := context.WithCancel(context.Background())
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, os.Interrupt)

    count := 0
    go func() {
        for msg := range partition.Messages() {
            count++
            fmt.Printf("Nhận message: key=%s, value=%s, offset=%d\n",
                string(msg.Key), string(msg.Value), msg.Offset)
            if count >= 2 {
                fmt.Println("Đã thấy 2 message – có thể có duplicate!")
            }
        }
    }()

    <-sig
    cancel()
    fmt.Printf("Tổng số message nhận được: %d\n", count)
}
```

Chạy consumer sau khi producer hoàn tất: chỉ nhận được 1 message, dù producer đã gửi retry. Điều này chứng minh idempotent đã loại bỏ duplicate.

## 7. Kiểm tra log segment của Kafka (tuỳ chọn)

```bash
# Xem các file log trong thư mục dữ liệu của Kafka
ls /var/lib/kafka/data/financial-0/
# Dùng công cụ kafka-dump-log để đọc nội dung segment
kafka-dump-log.sh --files /var/lib/kafka/data/financial-0/00000000000000000000.log --print-data-log
```

Bạn sẽ chỉ thấy 1 record với key và value tương ứng, dù producer đã gửi retry.

## 8. Giải thích kết quả và những điều học được

### a) Cơ chế hoạt động của Idempotent Producer

- Khi Producer khởi tạo, nó gửi request lên broker để nhận Producer ID.
- Mỗi message gửi đi mang theo Producer ID và Sequence Number.
- Broker theo dõi sequence number mới nhất cho mỗi PID-partition. Nếu message mới có sequence number ≤ sequence đã commit → bỏ qua. Nếu message có sequence lớn hơn đúng 1 đơn vị → chấp nhận. Nếu lớn hơn > 1 → lỗi (OutOfOrderSequenceException).
- Trong quá trình retry, Producer gửi lại message với cùng sequence number. Broker thấy trùng lặp → không ghi.

### b) Tầm quan trọng của `acks=all` và `max.in.flight.requests.per.connection`

- `acks=all` đảm bảo message được ghi vào tất cả ISR trước khi ack → bền vững.
- `max.in.flight.requests.per.connection=1` (hoặc ≤ 5 với idempotent) đảm bảo thứ tự và tránh lỗ hổng sequence number.

### c) Phân biệt với deduplication logic khác

- Idempotent Producer chỉ chống duplicate do retry nội bộ của producer (cùng PID, cùng sequence).
- Nếu ứng dụng gọi `send()` hai lần với cùng nội dung, đó là hai message khác nhau và sẽ được ghi hai lần (vì sequence khác nhau).
- Để chống duplicate từ logic ứng dụng, cần thêm Idempotent Consumer (ghi DB có kiểm tra khóa duy nhất) hoặc Kafka Transactions (Exactly-Once Semantics liên producer-consumer).

### d) Ứng dụng thực tế

- Các hệ thống tài chính, thanh toán: đảm bảo không ghi nhận 2 lần cùng một giao dịch do retry mạng.
- Kết hợp với Transaction API để đọc – xử lý – ghi chính xác một lần.

## 9. Mở rộng (tùy chọn)

- Thử nghiệm với `max.in.flight.requests.per.connection=5` (mặc định của idempotent) để thấy vẫn loại bỏ trùng lặp, nhưng thứ tự có thể không đảm bảo nếu có retry.
- Dùng `kafka-consumer-groups` để kiểm tra offset của consumer group sau khi consumer đọc xong – offset chỉ tiến 1 đơn vị.
- Viết unit test dùng Sarama's MockBroker để kiểm tra idempotent mà không cần Kafka thật.
