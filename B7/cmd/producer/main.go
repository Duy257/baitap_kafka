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
    config.Net.MaxOpenRequests = 1
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