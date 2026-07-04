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
	// Cấu hình consumer: round-robin rebalance, đọc từ đầu
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	brokers := []string{"localhost:9092"}
	group := "demo-group"

	consumerGroup, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Fatalf("Tạo consumer group lỗi: %v", err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())

	handler := &ConsumerHandler{}
	// Vòng lặp consume liên tục, tự động rejoin khi rebalance
	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{"payment"}, handler); err != nil {
				log.Printf("Lỗi consume: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Chờ tín hiệu Ctrl+C để thoát
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	cancel()
	fmt.Println("Consumer dừng.")
}

type ConsumerHandler struct{}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Đọc message từ partition, in ra partition/offset/key/value
	for msg := range claim.Messages() {
		fmt.Printf("Partition=%d, Offset=%d, Key=%s, Value=%s\n",
			msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		session.MarkMessage(msg, "")
	}
	return nil
}
