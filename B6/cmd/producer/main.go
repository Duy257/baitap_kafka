// Chương trình Producer gửi 100 message vào Kafka topic "orders".
// Mỗi message có key là số thứ tự (1-100) và value là "Message X".
// Yêu cầu: message số chẵn sẽ bị consumer giả lập lỗi DB để kiểm tra manual commit.

package main

import (
    "fmt"
    "log"

    "github.com/IBM/sarama"
)

func main() {
    // Khởi tạo cấu hình producer
    config := sarama.NewConfig()

    // Yêu cầu tất cả bản sao (ISR) xác nhận trước khi coi là thành công
    config.Producer.RequiredAcks = sarama.WaitForAll

    // Bật chế độ nhận kết quả gửi message (cần cho SyncProducer)
    config.Producer.Return.Successes = true

    // Tạo SyncProducer: gửi message đồng bộ, chờ kết quả trả về
    producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
    if err != nil {
        log.Fatalf("Tạo producer lỗi: %v", err)
    }

    // Đảm bảo đóng producer khi kết thúc, log lỗi nếu có
    defer func() {
        if err := producer.Close(); err != nil {
            log.Printf("Lỗi đóng producer: %v", err)
        }
    }()

    // Gửi 100 message vào topic "orders"
    topic := "orders"
    for i := 1; i <= 100; i++ {
        // Value: "Message 1", "Message 2", ...
        value := fmt.Sprintf("Message %d", i)

        // Key: số thứ tự dạng string, giúp message cùng key vào cùng partition
        key := sarama.StringEncoder(fmt.Sprintf("%d", i))

        // Tạo message với topic, key và value
        msg := &sarama.ProducerMessage{
            Topic: topic,
            Key:   key,
            Value: sarama.StringEncoder(value),
        }

        // Gửi message đồng bộ: trả về partition, offset, lỗi
        _, _, err := producer.SendMessage(msg)
        if err != nil {
            log.Printf("Gửi message %d lỗi: %v", i, err)
        }
    }

    fmt.Println("Đã gửi 100 message.")
}