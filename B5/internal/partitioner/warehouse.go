// Package partitioner cung cấp custom partitioner cho Sarama.
//
// Partitioner này ánh xạ warehouse_zone (key của message) vào partition
// theo logic nghiệp vụ:
//   - NORTH / SOUTH → Partition 0 (cùng khu vực miền Bắc + miền Trung)
//   - EAST           → Partition 1 (miền Đông)
//   - WEST           → Partition 2 (miền Tây)
//   - zone không xác định → FNV-1a hash % numPartitions
//   - nil / rỗng          → random (round-robin)
//
// Key được chuẩn hoá thành IN HOA trước khi xử lý, đảm bảo "north" và "NORTH"
// cùng vào một partition.
//
// Thread-safe: Partition() có thể được Sarama gọi concurrent từ nhiều
// dispatcher goroutines, do đó Warehouse dùng sync.Mutex bảo vệ random source.
package partitioner

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Warehouse implements sarama.Partitioner với logic phân phối theo vùng địa lý.
//
// Cấu trúc này duy trì một random source riêng (không dùng global rand)
// để tránh contention, và dùng Mutex để đảm bảo thread-safe khi Sarama
// gọi Partition() từ nhiều goroutine đồng thời.
type Warehouse struct {
	mu     sync.Mutex  // Bảo vệ random khỏi race condition (Sarama gọi concurrent)
	random *rand.Rand  // Random source riêng, seeded bằng UnixNano
}

// Partition quyết định partition cho message dựa trên key (warehouse_zone).
//
// Luồng xử lý:
//  1. Kiểm tra numPartitions hợp lệ (tránh panic khi % 0)
//  2. Key nil hoặc rỗng → random partition
//  3. Key không nil → Encode() lấy []byte, chuẩn hoá Upper, switch-case
//  4. NORTH/SOUTH → 0, EAST → 1, WEST → 2, default → hash
//
// Sarama gọi method này cho mỗi message trước khi gửi.
func (p *Warehouse) Partition(msg *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	// --- Guard: numPartitions <= 0 sẽ gây panic ở phép % ---
	if numPartitions <= 0 {
		return 0, fmt.Errorf("invalid number of partitions: %d", numPartitions)
	}

	// --- Key nil → random partition ---
	// Key nil thường xảy ra khi producer không set Key cho message.
	// Dùng random để tránh dồn hết vào partition 0.
	if msg.Key == nil {
		p.mu.Lock()
		part := p.random.Int31n(numPartitions)
		p.mu.Unlock()
		return part, nil
	}

	// --- Encode key bytes ---
	// msg.Key là sarama.Encoder interface, không phải []byte trực tiếp.
	// Cần gọi Encode() để lấy dữ liệu thật.
	keyBytes, err := msg.Key.Encode()
	if err != nil || len(keyBytes) == 0 {
		// Encode lỗi hoặc key rỗng → xử lý như nil key
		p.mu.Lock()
		part := p.random.Int31n(numPartitions)
		p.mu.Unlock()
		return part, nil
	}

	// --- Chuẩn hoá key thành IN HOA ---
	// Đảm bảo "north" và "NORTH" cùng vào partition 0.
	zone := strings.ToUpper(string(keyBytes))

	// --- Ánh xạ zone → partition theo logic nghiệp vụ ---
	switch zone {
	case "NORTH", "SOUTH":
		// NORTH và SOUTH gom chung partition 0 →
		// cùng consumer instance xử lý, tận dụng locality.
		return 0, nil
	case "EAST":
		return 1, nil
	case "WEST":
		return 2, nil
	default:
		// Zone lạ (UNKNOWN, ...) → dùng FNV-1a hash phân phối đều.
		// Tránh dồn hết vào một partition gây mất cân bằng tải.
		h := fnv.New32a()
		h.Write(keyBytes) // hash.Hash.Write never returns error
		return int32(h.Sum32()) % numPartitions, nil
	}
}

// RequiresConsistency trả về true báo hiệu Sarama rằng partitioner nhất quán
// theo key: cùng key luôn vào cùng partition.
//
// Khi true, Sarama có thể tối ưu bằng cách không gọi Partition() lại cho
// các message cùng key trong cùng batch.
func (p *Warehouse) RequiresConsistency() bool {
	return true
}

// NewWarehousePartitioner là constructor tạo WarehousePartitioner mới.
//
// Hàm này có chữ ký func(topic string) sarama.Partitioner, đúng kiểu
// Sarama yêu cầu cho config.Producer.Partitioner.
//
// Mỗi lần gọi tạo một instance riêng với random source riêng,
// seeded bằng UnixNano → khác nhau mỗi lần khởi động producer.
func NewWarehousePartitioner(topic string) sarama.Partitioner {
	return &Warehouse{
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}
