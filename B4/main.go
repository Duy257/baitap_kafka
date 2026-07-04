package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// === Định nghĩa tham số dòng lệnh ===
	broker := flag.String("broker", "localhost:9092", "Kafka broker address")
	topic := flag.String("topic", "throughput_test", "Kafka topic name")
	batchSize := flag.Int("batch-size", 0, "Batch size in bytes (0 = no threshold)")
	lingerMs := flag.Int("linger", 0, "Linger time in ms (0 = no delay)")
	compression := flag.String("compression", "none", "Compression: none, snappy, gzip, lz4, zstd")
	totalMessages := flag.Int("messages", 1000000, "Number of messages to send")
	flag.Parse()

	// === Kiểm tra giá trị đầu vào hợp lệ ===
	if *batchSize < 0 {
		log.Fatal("batch-size must be >= 0")
	}
	if *lingerMs < 0 {
		log.Fatal("linger must be >= 0")
	}
	if *totalMessages <= 0 {
		log.Fatal("messages must be > 0")
	}
	if *broker == "" {
		log.Fatal("broker must not be empty")
	}
	if *topic == "" {
		log.Fatal("topic must not be empty")
	}

	// === Cấu hình Kafka producer ===
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll         // Chờ tất cả replica ack
	config.Producer.Return.Successes = true                   // Báo cáo message gửi thành công
	config.Producer.Return.Errors = true                      // Báo cáo message gửi lỗi

	// === Cấu hình batch & linger ===
	config.Producer.Flush.Bytes = *batchSize                  // Ngưỡng kích thước batch (bytes)
	config.Producer.Flush.Frequency = time.Duration(*lingerMs) * time.Millisecond // Thời gian chờ gom batch
	config.Producer.Flush.MaxMessages = 0                     // Không giới hạn số message trong batch

	// === Cấu hình nén ===
	switch *compression {
	case "none":
		config.Producer.Compression = sarama.CompressionNone
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		log.Fatalf("unsupported compression: %s (supported: none, snappy, gzip, lz4, zstd)", *compression)
	}

	// === Khởi tạo async producer ===
	producer, err := sarama.NewAsyncProducer([]string{*broker}, config)
	if err != nil {
		log.Fatalf("Tạo producer thất bại: %v", err)
	}

	// === Biến đếm kết quả ===
	var (
		successCount int64 // Số message gửi thành công
		errorCount   int64 // Số message gửi lỗi
		wg           sync.WaitGroup
	)

	// === Goroutine đếm success ===
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successCount++
		}
	}()

	// === Goroutine đếm error ===
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Printf("Lỗi: %v\n", err)
			errorCount++
		}
	}()

	// === Xử lý tín hiệu dừng (Ctrl+C) ===
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// === Nội dung message mẫu ===
	messageValue := "test message for batching & compression demo - payload đủ dài"

	// === In thông tin cấu hình ===
	fmt.Printf("Bắt đầu gửi %d message với cấu hình:\n", *totalMessages)
	fmt.Printf("  broker=%s, topic=%s\n", *broker, *topic)
	fmt.Printf("  batch.size=%d, linger.ms=%d, compression=%s\n\n", *batchSize, *lingerMs, *compression)

	start := time.Now()

	// === Vòng lặp gửi message ===
	for i := 0; i < *totalMessages; i++ {
		select {
		case producer.Input() <- &sarama.ProducerMessage{
			Topic: *topic,
			Value: sarama.StringEncoder(messageValue),
		}:
		// Gửi message vào channel input của producer
		case <-ctx.Done():
			// Nhận tín hiệu dừng -> thoát vòng lặp
			log.Printf("Nhận tín hiệu dừng sau khi gửi %d message", i)
			goto shutdown
		}
	}

shutdown:
	// === Đóng producer và chờ goroutine xử lý xong ===
	producer.AsyncClose()
	wg.Wait()
	elapsed := time.Since(start)

	// === Tính toán và in kết quả ===
	throughput := float64(successCount) / elapsed.Seconds()
	rawBytes := int64(*totalMessages * len(messageValue))
	fmt.Println("================ KẾT QUẢ ================")
	fmt.Printf("Thành công: %d, Lỗi: %d\n", successCount, errorCount)
	fmt.Printf("Thời gian: %.3f giây\n", elapsed.Seconds())
	fmt.Printf("Throughput: %.2f msg/s\n", throughput)
	fmt.Printf("Dữ liệu thô (không nén): %d bytes (~%.2f MB)\n", rawBytes, float64(rawBytes)/(1024*1024))
	fmt.Printf("Ghi chú: với compression=%s, dữ liệu thực tế trên mạng nhỏ hơn nhiều.\n", *compression)
}
