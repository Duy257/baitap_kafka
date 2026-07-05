// Chương trình Consumer xử lý message với manual offset commit.
// Mô phỏng ghi DB: message số lẻ thành công, số chẵn retry tối đa 3 lần.
// Nếu commit thất bại, offset không được đánh dấu -> sẽ xử lý lại khi restart.
// Đây là cơ chế Exactly-Once Semantics phía consumer kết hợp idempotent bên ngoài.

package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/IBM/sarama"
)

const dlqTopic = "orders-dlq"

var errMaxRetriesExceeded = errors.New("max retries exceeded, send to DLQ")

// retryMap lưu số lần retry của từng message (key = nội dung message).
// Dùng để giả lập: sau 3 lần retry, message bị đẩy vào DLQ.
// Lưu ý: đây là bộ nhớ trong, mất khi restart -> consumer sẽ retry lại từ đầu.
var (
    mu       sync.Mutex
    retryMap = make(map[string]int)
)

// processMessage xử lý một message từ Kafka, giả lập ghi vào database.
// - Nếu message là số lẻ: luôn thành công.
// - Nếu message là số chẵn: retry tối đa 3 lần, lần thứ 4 vào DLQ.
// Trả về nil nếu xử lý thành công, error nếu thất bại (cần retry).
// Trả về errMaxRetriesExceeded nếu quá số lần retry cho phép.
func processMessage(msg *sarama.ConsumerMessage) error {
    // Chuyển []byte thành string để xử lý
    msgStr := string(msg.Value)

    // Parse số thứ tự từ value "Message X"
    parts := strings.Split(msgStr, " ")
    if len(parts) < 2 {
        return fmt.Errorf("invalid message format")
    }
    num, err := strconv.Atoi(parts[1])
    if err != nil {
        return fmt.Errorf("parse error: %v", err)
    }

    // Message số chẵn: giả lập lỗi DB
    if num%2 == 0 {
        mu.Lock()
        retries := retryMap[msgStr]
        retries++
        retryMap[msgStr] = retries
        mu.Unlock()

        // 3 lần đầu: trả về lỗi để consumer retry
        if retries <= 3 {
            return fmt.Errorf("DB error for message %d (retry %d)", num, retries)
        }
        // Lần thứ 4: quá số lần retry, chuyển sang DLQ
        log.Printf("Message %d: max retries (%d) exceeded, moving to DLQ", num, retries)
        return fmt.Errorf("%w: message %d", errMaxRetriesExceeded, num)
    }

    log.Printf("Processed successfully: %s", msg.Value)
    return nil
}

// ConsumerHandler implement sarama.ConsumerGroupHandler để xử lý message theo consumer group.
// Các phương thức: Setup, Cleanup, ConsumeClaim.
type ConsumerHandler struct {
    ctx         context.Context        // context để kiểm tra signal dừng trong retry loop
    ready       chan bool              // báo hiệu consumer group đã sẵn sàng
    dlqProducer sarama.SyncProducer    // producer gửi message vào DLQ sau 3 lần retry
}

// Setup được gọi khi consumer group bắt đầu session (sau join group, trước khi nhận partition).
// Đóng channel ready để thông báo cho main goroutine rằng consumer đã sẵn sàng.
func (c *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
    close(c.ready)
    return nil
}

// Cleanup được gọi khi consumer group kết thúc session (khi rebalance hoặc shutdown).
func (c *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
    return nil
}

// ConsumeClaim xử lý message từ một partition được gán cho consumer này.
// Sarama chạy phương thức này trong một goroutine riêng cho mỗi partition.
// Điểm chính: chỉ commit offset SAU KHI xử lý thành công (manual commit).
func (c *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    // Lặp qua từng message trong partition (channel bị đóng khi session kết thúc)
    for msg := range claim.Messages() {
        log.Printf("Received: %s (partition=%d, offset=%d)", msg.Value, msg.Partition, msg.Offset)

        // retryLoop: thử lại cho đến khi xử lý thành công hoặc context bị cancel
    retryLoop:
        for {
            // Kiểm tra context cancellation (Ctrl+C) để thoát sớm, không block shutdown
            select {
            case <-c.ctx.Done():
                return c.ctx.Err()
            default:
            }

            // Xử lý message
            err := processMessage(msg)
            if err == nil {
                // Xử lý thành công:
                // MarkMessage đánh dấu offset trong bộ nhớ (chưa commit ngay)
                session.MarkMessage(msg, "")
                // Commit() đẩy offset đã mark lên Kafka ngay lập tức (đồng bộ)
                session.Commit()
                break retryLoop
            } else if errors.Is(err, errMaxRetriesExceeded) {
                // Quá số lần retry: gửi message vào DLQ, commit offset để không xử lý lại
                dlqMsg := &sarama.ProducerMessage{
                    Topic: dlqTopic,
                    Key:   sarama.ByteEncoder(msg.Key),
                    Value: sarama.ByteEncoder(msg.Value),
                }
                _, _, dlqErr := c.dlqProducer.SendMessage(dlqMsg)
                if dlqErr != nil {
                    log.Printf("Failed to send to DLQ: %v", dlqErr)
                } else {
                    log.Printf("Sent to DLQ: %s", msg.Value)
                }
                session.MarkMessage(msg, "")
                session.Commit()
                break retryLoop
            } else {
                // Xử lý thất bại: log lỗi, chờ 1 giây rồi thử lại
                // Không MarkMessage, không Commit -> offset giữ nguyên
                // Nếu consumer crash, khi restart sẽ đọc lại message này
                log.Printf("Error processing %s: %v. Retrying in 1s...", msg.Value, err)
                time.Sleep(1 * time.Second)
            }
        }
    }
    return nil
}

func main() {
    // ===== Cấu hình Kafka Consumer =====
    config := sarama.NewConfig()

    // Chiến lược phân phối partition: RoundRobin
    config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

    // Đọc từ đầu topic nếu chưa có offset (lần đầu chạy)
    config.Consumer.Offsets.Initial = sarama.OffsetOldest

    // TẮT auto-commit: consumer tự quyết định khi nào commit offset
    // Đây là yêu cầu bắt buộc của bài tập để kiểm soát thời điểm commit
    config.Consumer.Offsets.AutoCommit.Enable = false

    // Tăng session timeout để tránh bị Kafka coi là "chết" khi retry lâu
    config.Consumer.Group.Session.Timeout = 30 * time.Second

    // Heartbeat interval: gửi tín hiệu sống mỗi 3 giây
    config.Consumer.Group.Heartbeat.Interval = 3 * time.Second

    broker := []string{"localhost:9092"}
    group := "manual-commit-group"

    // Tạo consumer group
    consumerGroup, err := sarama.NewConsumerGroup(broker, group, config)
    if err != nil {
        log.Fatalf("Tạo consumer group lỗi: %v", err)
    }

    // Đóng consumer group khi kết thúc, log lỗi nếu có
    defer func() {
        if err := consumerGroup.Close(); err != nil {
            log.Printf("Lỗi đóng consumer group: %v", err)
        }
    }()

    // Tạo producer riêng để gửi message vào DLQ
    dlqConfig := sarama.NewConfig()
    dlqConfig.Producer.RequiredAcks = sarama.WaitForAll
    dlqConfig.Producer.Return.Successes = true
    dlqProducer, err := sarama.NewSyncProducer(broker, dlqConfig)
    if err != nil {
        log.Fatalf("Tạo DLQ producer lỗi: %v", err)
    }
    defer func() {
        if err := dlqProducer.Close(); err != nil {
            log.Printf("Lỗi đóng DLQ producer: %v", err)
        }
    }()

    // Context dùng để cancel consumer group khi nhận Ctrl+C
    ctx, cancel := context.WithCancel(context.Background())

    // Tạo handler chứa context, ready channel và DLQ producer
    handler := &ConsumerHandler{ctx: ctx, ready: make(chan bool), dlqProducer: dlqProducer}

    // Goroutine riêng để chạy consumer group vòng lặp
    // Khi Consume trả về (rebalance hoặc lỗi), nếu context chưa cancel thì chạy lại
    go func() {
        for {
            if err := consumerGroup.Consume(ctx, []string{"orders"}, handler); err != nil {
                log.Printf("Lỗi consume: %v", err)
            }
            if ctx.Err() != nil {
                return
            }
        }
    }()

    // Chờ consumer group sẵn sàng (Setup đã đóng channel ready)
    <-handler.ready
    log.Println("Consumer đã sẵn sàng, nhấn Ctrl+C để dừng.")

    // ===== Chờ tín hiệu dừng (Ctrl+C) =====
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, os.Interrupt)
    <-sig

    // Gửi tín hiệu cancel để consumer group dừng
    cancel()
    log.Println("Consumer dừng.")
}