# Bài 5: Custom Partitioner theo vùng địa lý

**Mục tiêu:** Áp dụng logic business vào phân phối dữ liệu.

## Yêu cầu

- Topic `inventory`.
- Message có field `warehouse_zone` (ví dụ: `"NORTH"`, `"SOUTH"`, `"EAST"`, `"WEST"`).
- Viết Custom Partitioner sao cho:

| Zone             | Partition |
| ---------------- | --------- |
| `NORTH`, `SOUTH` | 0         |
| `EAST`           | 1         |
| `WEST`           | 2         |

**Lưu ý:** Code Partitioner phải handle trường hợp key `null` hoặc zone lạ.

## 1. Mục tiêu bài học

- Hiểu cách Kafka chọn partition dựa trên key và custom partitioner.
- Tự xây dựng một partitioner tùy chỉnh theo logic nghiệp vụ (phân vùng địa lý).
- Xử lý các tình huống đặc biệt: key `null`, giá trị không xác định.
- Biết cách cấu hình Sarama Producer sử dụng partitioner riêng.

## 2. Kiến thức nền tảng

- **Partitioner mặc định** của Kafka (Java client): nếu `key != null` → `murmur2(key) % numPartitions`; nếu `key == null` → round-robin (sticky partitioning từ 2.4). Với Sarama, mặc định là `sarama.NewHashPartitioner`.
- **Custom Partitioner**: implement interface `sarama.Partitioner` với method:
  ```go
  Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error)
  ```
- **Ứng dụng**: đảm bảo các message cùng vùng địa lý (NORTH, SOUTH, EAST, WEST) vào cùng partition → thứ tự, locality, tối ưu consumer.

**Lưu ý:** Vì `Partition()` chỉ nhận `message.Key` (`[]byte`), nên ta sẽ dùng `warehouse_zone` làm key. Nếu muốn truyền trong value, cần parse lại nhưng không khả thi với interface này – đây là chủ ý thiết kế của Kafka.
