// Command producer gửi message mẫu vào topic "inventory" với custom partitioner.
//
// Usage:
//   go run ./cmd/producer
//   go build -o producer.exe ./cmd/producer && ./producer.exe
//
// Mỗi message có key là warehouse_zone (NORTH, SOUTH, EAST, WEST, ...)
// và value là dữ liệu hàng hoá. Custom partitioner trong
// internal/partitioner sẽ quyết định partition dựa trên key.
//
// Kết quả hiển thị: Key -> Partition, Offset, Value.
package main

import (
	"fmt"
	"log"
	"math/rand"

	"github.com/IBM/sarama"
	"github.com/duynguyen/kafka-b5/internal/partitioner"
)

func main() {
	// ====================================================================
	// Cấu hình Sarama Producer
	// ====================================================================
	config := sarama.NewConfig()

	// Chỉ gửi thành công khi tất cả ISR đã ghi nhận message.
	// Đảm bảo độ tin cậy cao nhất (cao hơn mặc định).
	config.Producer.RequiredAcks = sarama.WaitForAll

	// SyncProducer cần Return.Successes = true để nhận kết quả gửi.
	config.Producer.Return.Successes = true

	// Gán custom partitioner: tất cả message vào topic "inventory" sẽ
	// được phân phối theo logic warehouse_zone.
	// NewWarehousePartitioner có chữ ký func(topic string) sarama.Partitioner.
	config.Producer.Partitioner = partitioner.NewWarehousePartitioner

	// ====================================================================
	// Khởi tạo SyncProducer
	// ====================================================================
	// SyncProducer gửi message đồng bộ: SendMessage block đến khi có kết quả.
	// Phù hợp cho bài tập demo, không dùng cho production throughput cao.
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Tạo producer thất bại: %v", err)
	}
	defer producer.Close()

	topic := "inventory"

	// ====================================================================
	// Gửi 1000 message ngẫu nhiên
	// ====================================================================
	zones := []string{"NORTH", "SOUTH", "EAST", "WEST", "UNKNOWN", ""}

	for i := 1; i <= 1000; i++ {
		key := zones[rand.Intn(len(zones))]
		value := fmt.Sprintf("inventory_data_%d", i)

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
		}

		if key != "" {
			msg.Key = sarama.StringEncoder(key)
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Lỗi gửi message %d: %v", i, err)
			continue
		}

		fmt.Printf("Key=%-10s -> Partition=%d, Offset=%d, Value=%s\n",
			key, partition, offset, value)
	}

	fmt.Println("Hoàn tất gửi 1000 message.")
}
