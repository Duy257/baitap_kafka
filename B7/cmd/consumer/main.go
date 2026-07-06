package main

import (
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
    fmt.Printf("Tổng số message nhận được: %d\n", count)
}