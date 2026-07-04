// Package main — chương trình Consumer Kafka.
// Chạy trong consumer group "order-group", subscribe topic "orders".
// Mục đích: minh hoạ cơ chế consumer-group rebalancing
// (phân phối lại partition khi consumer join/leave).
package main

import (
	"context"      // Hủy bỏ tác vụ (cancel), quản lý vòng đời goroutine
	"flag"          // Parse tham số dòng lệnh
	"fmt"           // In log ra stdout
	"log"           // Ghi log lỗi
	"os"            // Tín hiệu hệ thống (SIGINT, SIGTERM)
	"os/signal"
	"sync" // sync.Once — đảm bảo ready channel chỉ được đóng 1 lần
	"syscall"

	// Thư viện Sarama — Go client chính thức cho Kafka
	"github.com/IBM/sarama"
)

// --------------------- Cấu hình mặc định -------------------------

const (
	// Địa chỉ Kafka broker
	defaultBrokers = "localhost:9092"
	// Topic consumer sẽ lắng nghe
	defaultTopic = "orders"
	// Consumer group ID — tất cả consumer cùng group sẽ chia sẻ partition
	defaultGroup = "order-group"
)

// ConsumerGroupHandler — implement sarama.ConsumerGroupHandler để
// xử lý message từ Kafka consumer group.
//
// Gồm 3 lifecycle method:
//   - Setup:  được gọi khi consumer được gán partition (bắt đầu consume)
//   - Cleanup: được gọi khi partition bị thu hồi (rebalance xảy ra)
//   - ConsumeClaim: vòng lặp xử lý message từ một partition cụ thể
//
// Các sự kiện rebalance được log ra stdout để người dùng quan sát
// cách Kafka phân phối lại partition khi consumer join/leave group.
type ConsumerGroupHandler struct {
	ready chan struct{}   // Báo hiệu consumer đã sẵn sàng (đóng channel khi Setup chạy xong)
	once  sync.Once       // Đảm bảo close(ready) chỉ được gọi 1 lần, tránh panic
}

// Setup — được Kafka gọi khi consumer lần đầu được gán partition,
// hoặc sau mỗi lần rebalance (có partition mới được gán).
//
// Hành vi:
//   - In danh sách partition được gán → giúp quan sát rebalance
//   - Signal ready channel lần đầu tiên (dùng sync.Once để không panic nếu re-run)
func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	claims := session.Claims() // Lấy map[topic][]partition được gán
	for topic, partitions := range claims {
		fmt.Printf("[REBALANCE] Consumer %s | Assigned %s → %v\n",
			session.MemberID(), topic, partitions)
	}

	// Đóng ready channel — chỉ lần đầu (initial startup).
	// Khi rebalance, consumer sẽ được re-create handler mới
	// nên sync.Once không cần đặt lại.
	h.once.Do(func() {
		close(h.ready)
	})
	return nil
}

// Cleanup — được Kafka gọi khi partition bị thu hồi,
// tức là một rebalance đang diễn ra.
//
// Consumer hiện tại sắp mất một số partition,
// chúng sẽ được gán lại cho consumer khác trong group.
func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("[REBALANCE] Consumer %s | Partitions revoked, rebalancing...\n",
		session.MemberID())
	return nil
}

// ConsumeClaim — xử lý message từ một partition cụ thể.
// Đây là vòng lặp chính: đọc message từ channel, in ra màn hình và đánh dấu đã xử lý.
//
// Lưu ý:
//   - claim.Messages() trả về channel chỉ đọc — đọc đến khi channel đóng
//   - session.MarkMessage() báo Kafka rằng message đã được xử lý xong (commit offset)
func (h *ConsumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	fmt.Printf("[CONSUME] Consumer %s | Listening on partition %d\n",
		session.MemberID(), claim.Partition())

	// Vòng lặp đọc message từ partition cho đến khi channel đóng
	for msg := range claim.Messages() {
		fmt.Printf("[RECV] %s | Partition %d | Offset %d | Value: %s\n",
			session.MemberID(), msg.Partition, msg.Offset, string(msg.Value))
		session.MarkMessage(msg, "") // Đánh dấu message đã xử lý (commit offset)
	}
	return nil
}

// main — điểm vào của chương trình Consumer.
//
// Quy trình:
//  1. Parse tham số dòng lệnh (brokers, topic, group, verbose)
//  2. Tạo ConsumerGroup với Sarama (RoundRobin strategy, đọc từ đầu)
//  3. Chạy vòng lặp consume trong goroutine — tự động re-join sau mỗi rebalance
//  4. Chờ tín hiệu dừng (SIGINT/SIGTERM), shutdown sạch sẽ
func main() {
	// ----------------- Đọc tham số dòng lệnh ----------------------
	brokers := flag.String("brokers", defaultBrokers, "Kafka broker(s), comma-separated")
	topic := flag.String("topic", defaultTopic, "Topic to consume from")
	group := flag.String("group", defaultGroup, "Consumer group ID")
	verbose := flag.Bool("v", false, "Verbose Sarama logging")
	flag.Parse()

	// In thông tin khởi động
	fmt.Printf("=== Consumer starting ===\n  group=%s\n  brokers=%s\n  topic=%s\n\n",
		*group, *brokers, *topic)

	// Bật log chi tiết của Sarama (debug heartbeat, rebalance, commit offset,...)
	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	// ----------------- Cấu hình Sarama Consumer -------------------
	config := sarama.NewConfig()

	// Chiến lược phân phối partition: Round-Robin
	// Kafka sẽ lần lượt gán partition cho từng consumer trong group
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	// Bắt đầu đọc từ offset đầu tiên (message cũ nhất còn trên topic)
	// Nếu không set, Sarama mặc định là OffsetNewest (chỉ đọc message mới)
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Phiên bản Kafka cluster — cần set để Sarama dùng đúng giao thức
	config.Version = sarama.V2_6_0_0

	// Tạo consumer group — Sarama tự động quản lý heartbeat, rebalance, commit
	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{*brokers}, *group, config,
	)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	// Đảm bảo đóng consumer group khi chương trình kết thúc
	defer consumerGroup.Close()

	// Tạo context có thể cancel — dùng để ngừng consume khi shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	// Tạo handler với ready channel (sẽ được close khi Setup chạy lần đầu)
	handler := &ConsumerGroupHandler{ready: make(chan struct{})}

	// ----------------- Vòng lặp consume (background) -------------
	// Goroutine này chạy mãi cho đến khi context bị cancel.
	// Mỗi lần rebalance xảy ra, consumerGroup.Consume() trả về,
	// và vòng lặp while sẽ gọi lại Consume() để re-join group.
	go func() {
		defer wg.Done()
		for {
			// Consume — blocking call, chạy cho đến khi:
			//   - context bị cancel  → consumer group shutdown
			//   - rebalance xảy ra   → trả về, handler bị vô hiệu
			if err := consumerGroup.Consume(ctx, []string{*topic}, handler); err != nil {
				log.Printf("Consume error: %v", err)
			}

			// Nếu context đã bị cancel, thoát vòng lặp
			if ctx.Err() != nil {
				return
			}

			// Rebalance vừa xảy ra → cần tạo handler mới.
			// Handler cũ đã dùng hết (Setup đã chạy, Cleanup đã chạy, ready channel đã đóng).
			// Handler mới với ready channel mới sẽ được Setup lại khi Consume() chạy.
			handler = &ConsumerGroupHandler{ready: make(chan struct{})}
		}
	}()

	// Chờ handler báo ready (Setup đã chạy, partition đã được gán)
	<-handler.ready
	fmt.Println("\nConsumer ready, waiting for messages...")
	fmt.Println("(start another consumer instance to see rebalancing)")

	// ----------------- Chờ tín hiệu dừng -------------------------
	// Block main goroutine cho đến khi nhận SIGINT (Ctrl+C) hoặc SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Bắt đầu shutdown
	fmt.Println("\nShutting down consumer...")
	cancel()   // Hủy context → vòng lặp consume sẽ thoát
	wg.Wait()  // Chờ goroutine consume kết thúc
	fmt.Println("Consumer stopped.")
}
