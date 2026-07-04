// Package main — chương trình Producer Kafka.
// Gửi message tới topic "orders" để minh hoạ cơ chế
// consumer-group rebalancing (phân phối lại partition).
package main

import (
	"context"      // Hủy bỏ tác vụ (cancel) khi nhận tín hiệu dừng
	"encoding/json" // Mã hoá dữ liệu struct thành JSON
	"flag"          // Parse tham số dòng lệnh
	"fmt"           // In kết quả ra stdout
	"log"           // Ghi log lỗi
	"os"            // Tín hiệu hệ thống (SIGINT, SIGTERM)
	"os/signal"
	"syscall"
	"time" // Delay giữa các message

	// Thư viện Sarama — Go client chính thức cho Kafka
	"github.com/IBM/sarama"
)

// --------------------- Cấu hình mặc định -------------------------

const (
	// Địa chỉ Kafka broker(s), cách nhau bằng dấu phẩy
	defaultBrokers = "localhost:9092"
	// Topic mặc định để producer gửi message vào
	defaultTopic = "orders"
	// Số lượng message sẽ gửi (mặc định 1000)
	defaultCount = 1000
	// Thời gian delay giữa 2 message (2ms) — tránh quá tải
	sendDelay = 2 * time.Millisecond
)

// Order — cấu trúc dữ liệu của message.
// Mỗi message chứa một đơn hàng với ID, tên sản phẩm và số tiền.
// Được mã hoá thành JSON trước khi gửi lên Kafka.
type Order struct {
	OrderID int    `json:"order_id"` // Mã đơn hàng
	Product string `json:"product"`  // Tên sản phẩm (item_0..item_9)
	Amount  int    `json:"amount"`   // Giá trị đơn hàng (i * 100)
}

// main — điểm vào của chương trình Producer.
//
// Quy trình:
//  1. Parse tham số dòng lệnh (brokers, topic, count, verbose)
//  2. Tạo SyncProducer (gửi message đồng bộ — chờ ack)
//  3. Gửi lần lượt N message, mỗi message cách nhau 2ms
//  4. Dừng an toàn khi nhận SIGINT (Ctrl+C) hoặc SIGTERM
func main() {
	// ----------------- Đọc tham số dòng lệnh ----------------------
	brokers := flag.String("brokers", defaultBrokers, "Kafka broker(s), comma-separated")
	topic := flag.String("topic", defaultTopic, "Topic to produce to")
	count := flag.Int("count", defaultCount, "Number of messages to send")
	verbose := flag.Bool("v", false, "Verbose Sarama logging")
	flag.Parse()

	// In thông tin khởi động
	fmt.Printf("=== Producer starting ===\n  count=%d\n  topic=%s\n  brokers=%s\n\n",
		*count, *topic, *brokers)

	// Bật log chi tiết của Sarama (debug connection, metadata, ...)
	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	// ----------------- Cấu hình Sarama Producer -------------------
	config := sarama.NewConfig()
	// Yêu cầu Kafka xác nhận (ack) cho mỗi message gửi thành công
	config.Producer.Return.Successes = true
	// Trả về lỗi nếu gửi thất bại
	config.Producer.Return.Errors = true

	// Partitioner dạng Round-Robin — phân phối đều message sang các partition.
	// Mục đích: tạo đều dữ liệu trên tất cả partition,
	// giúp quan sát rõ rebalancing khi consumer join/leave.
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner

	// Tạo SyncProducer — gửi message đồng bộ, chờ ack từ broker
	producer, err := sarama.NewSyncProducer([]string{*brokers}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	// Đảm bảo đóng producer khi chương trình kết thúc
	defer producer.Close()

	// ----------------- Xử lý tín hiệu dừng ------------------------
	// Tạo context có khả năng cancel — dùng để notify goroutine dừng
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Lắng nghe SIGINT (Ctrl+C) và SIGTERM (kill) để dừng sạch
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived signal, stopping producer...")
		cancel() // Hủy context → các vòng lặp đang chạy sẽ thoát
	}()

	// ----------------- Vòng lặp gửi message -----------------------
	sent := 0 // Đếm số message đã gửi thành công
	for i := 1; i <= *count; i++ {
		// Kiểm tra xem có tín hiệu dừng không trước mỗi lần gửi
		select {
		case <-ctx.Done():
			fmt.Printf("Producer interrupted after %d messages.\n", sent)
			return
		default:
		}

		// Tạo payload — đơn hàng chứa ID, sản phẩm, số tiền
		order := Order{
			OrderID: i,
			Product: fmt.Sprintf("item_%d", i%10), // 10 sản phẩm luân phiên
			Amount:  i * 100,
		}
		value, _ := json.Marshal(order) // Mã hoá struct → JSON

		// Tạo message Kafka: topic + key (xác định partition) + value
		msg := &sarama.ProducerMessage{
			Topic: *topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("%d", i)),
			Value: sarama.StringEncoder(value),
		}

		// Gửi message đồng bộ — chờ Kafka trả về partition + offset
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
			continue // Gửi lỗi → bỏ qua, gửi message tiếp theo
		}

		// In kết quả: số thứ tự → partition nào, offset bao nhiêu
		fmt.Printf("Sent %3d → partition %d, offset %d\n", i, partition, offset)
		sent++

		// Delay nhẹ để quan sát quá trình consume từ từ
		time.Sleep(sendDelay)
	}

	fmt.Printf("\nDone! Sent %d messages to topic %s.\n", sent, *topic)
}
