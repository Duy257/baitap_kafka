// Command consumer đọc tất cả message từ topic "inventory"
// để xác nhận custom partitioner phân phối đúng partition.
//
// Usage:
//   go run ./cmd/consumer
//   go build -o consumer.exe ./cmd/consumer && ./consumer.exe
//
// Consumer đọc từ đầu (OffsetOldest) trên cả 3 partition, in ra
// Partition, Offset, Key, Value cho mỗi message. Dùng signal.NotifyContext
// để graceful shutdown khi nhấn Ctrl+C.
//
// Cần chạy producer trước đó để có message để consume.
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
	// ====================================================================
	// Cấu hình Sarama Consumer
	// ====================================================================
	config := sarama.NewConfig()

	// Bắt đầu đọc từ message đầu tiên (offset oldest).
	// Mặc định là OffsetNewest (chỉ đọc message mới), nhưng bài tập này
	// cần xem lại message đã gửi từ producer.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// ====================================================================
	// Khởi tạo Consumer
	// ====================================================================
	// Consumer này đọc message nhưng không thuộc consumer group —
	// nó đọc trực tiếp partition, phù hợp cho mục đích kiểm tra.
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Tạo consumer lỗi: %v", err)
	}
	defer consumer.Close()

	topic := "inventory"

	// Lấy danh sách partition của topic (0, 1, 2).
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("Lấy danh sách partition lỗi: %v", err)
	}

	// ====================================================================
	// Graceful shutdown với signal.NotifyContext
	// ====================================================================
	// Khi nhấn Ctrl+C, context bị cancel → tất cả goroutine consumer
	// nhận tín hiệu và dừng sạch sẽ (defer pc.Close() được gọi).
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// ====================================================================
	// Consume tất cả partition
	// ====================================================================
	// Mỗi partition chạy một goroutine riêng, đọc message từ đầu.
	for _, p := range partitions {
		// ConsumePartition trả về PartitionConsumer để đọc message
		// từ một partition cụ thể, bắt đầu từ offset chỉ định.
		pc, err := consumer.ConsumePartition(topic, p, sarama.OffsetOldest)
		if err != nil {
			log.Printf("Lỗi consume partition %d: %v", p, err)
			continue
		}
		// Mỗi partition consumer chạy trong goroutine riêng.
		go consumePartition(ctx, pc, p)
	}

	// Block main goroutine cho đến khi nhận signal (Ctrl+C).
	<-ctx.Done()
	fmt.Println("Consumer dừng.")
}

// consumePartition đọc message từ một PartitionConsumer và in ra stdout.
//
// Vòng lặp select:
//   - pc.Messages(): kênh message chính, đóng khi partition hết dữ liệu.
//   - pc.Errors():   kênh lỗi, log và tiếp tục (không dừng).
//   - ctx.Done():    signal thoát khi context bị cancel (Ctrl+C).
//
// defer pc.Close() đảm bảo giải phóng tài nguyên khi goroutine kết thúc.
func consumePartition(ctx context.Context, pc sarama.PartitionConsumer, partition int32) {
	defer pc.Close()

	for {
		select {
		// --- Kênh message chính ---
		case msg, ok := <-pc.Messages():
			if !ok {
				// Kênh đã đóng → partition không còn dữ liệu → thoát.
				return
			}
			// msg.Key có thể nil → string(msg.Key) trả về "".
			key := string(msg.Key)
			fmt.Printf("Partition=%d, Offset=%d, Key=%s, Value=%s\n",
				msg.Partition, msg.Offset, key, string(msg.Value))

		// --- Kênh lỗi (không bắt buộc, nhưng nên xử lý) ---
		case err := <-pc.Errors():
			// Lỗi có thể là timeout, mất kết nối, rebalance...
			// Chỉ log, không dừng consumer.
			log.Printf("Lỗi partition %d: %v", partition, err)

		// --- Tín hiệu dừng ---
		case <-ctx.Done():
			// Context bị cancel (Ctrl+C) → thoát goroutine sạch sẽ.
			return
		}
	}
}
