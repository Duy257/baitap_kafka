package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// Cấu hình producer đảm bảo thứ tự
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll       // Chờ tất cả replica ack
	config.Producer.Idempotent = true                      // Chống trùng lặp khi retry
	config.Net.MaxOpenRequests = 1                         // Chỉ 1 request in-flight → giữ thứ tự
	config.Producer.Retry.Max = 5                          // Số lần retry tối đa
	config.Producer.Return.Successes = true                // Cần để SyncProducer hoạt động

	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Khởi tạo producer lỗi: %v", err)
	}
	defer producer.Close()

	topic := "payment"

	// Gửi 50 message với key random
	// Key khác nhau → có thể vào các partition khác nhau
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("random-%d", rand.Intn(1000))
		value := fmt.Sprintf("transaction-%d", i)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(value),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Gửi message lỗi: %v", err)
		} else {
			fmt.Printf("Đã gửi: key=%s, value=%s -> partition=%d, offset=%d\n",
				key, value, partition, offset)
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("Hoàn tất gửi 50 message. Nhấn Ctrl+C để thoát.")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
}
