# Hướng dẫn Bài 5: Custom Partitioner theo vùng địa lý

## 1. Tạo topic

Tạo topic `inventory` với 3 partition:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic inventory \
  --partitions 3 --replication-factor 1
```

## 2. Build

```bash
# Build producer
go build -o producer.exe ./cmd/producer

# Build consumer
go build -o consumer.exe ./cmd/consumer

# Hoặc chạy trực tiếp
go run ./cmd/producer
go run ./cmd/consumer
```

**Cấu trúc project:**

```
B5/
├── cmd/
│   ├── producer/main.go       # Producer entry
│   └── consumer/main.go       # Consumer entry
├── internal/
│   └── partitioner/
│       └── warehouse.go       # Custom partitioner (package partitioner)
├── docs/
├── go.mod
└── go.sum
```

## 3. Custom partitioner (`internal/partitioner/warehouse.go`)

Viết custom partitioner ánh xạ `warehouse_zone` → partition theo logic:

| Zone                | Partition |
| ------------------- | --------- |
| `NORTH`, `SOUTH`    | 0         |
| `EAST`              | 1         |
| `WEST`              | 2         |
| `null` / rỗng       | random    |
| zone không xác định | hash      |

## 4. Producer

Producer gửi message với key là `warehouse_zone`.

Chạy:
```bash
go run ./cmd/producer
# hoặc build + chạy
go build -o producer.exe ./cmd/producer && ./producer.exe
```

## 5. Consumer

Xác nhận message vào đúng partition.

Chạy:
```bash
go run ./cmd/consumer
# hoặc
go build -o consumer.exe ./cmd/consumer && ./consumer.exe
```

## 6. Kết quả và phân tích

Chạy producer:

```text
Key=NORTH     -> Partition=0, Offset=..., Value=inventory_data_1
Key=SOUTH     -> Partition=0, Offset=..., Value=inventory_data_2
Key=EAST      -> Partition=1, Offset=..., Value=inventory_data_3
Key=WEST      -> Partition=2, Offset=..., Value=inventory_data_4
Key=north     -> Partition=0, Offset=..., Value=inventory_data_5
Key=SOUTH     -> Partition=0, Offset=..., Value=inventory_data_6
Key=UNKNOWN   -> Partition=?, Offset=... (hash-based)
Key=          -> Partition ngẫu nhiên (random)
```

### Nhận xét

- **NORTH** và **SOUTH** cùng partition 0 → đúng yêu cầu.
- **EAST** partition 1, **WEST** partition 2.
- Chữ thường được chuẩn hoá thành in hoa (nhờ `strings.ToUpper`).
- Key `null` / rỗng: được phân phối ngẫu nhiên hoặc theo chiến lược dự phòng (ở đây dùng random).
- Zone lạ: dùng hàm băm để phân tán, tránh dồn hết vào một partition.

## 7. Những điều học được từ bài tập

### a) Vai trò của Key trong Kafka

Key không chỉ để đảm bảo thứ tự (cùng key → cùng partition) mà còn là cơ sở cho logic phân phối tùy biến.

Custom partitioner cho phép ánh xạ key → partition theo ý đồ riêng (ví dụ: nhóm theo vùng địa lý, loại thiết bị, mức độ ưu tiên…).

### b) Thiết kế Partitioner trong Sarama

Interface `Partitioner` chỉ cần 2 method:

- `Partition()` – tính toán partition cho message.
- `RequiresConsistency()` – trả về `true` nếu partitioner muốn các message trong cùng batch có cùng key (để tránh phải tính lại partition cho từng message). Thường ta trả về `true` với partitioner phụ thuộc key.

Constructor `func(topic string) sarama.Partitioner` giúp tạo partitioner riêng cho từng topic nếu cần.

### c) Xử lý biên (null key, giá trị lạ)

- Luôn có chiến lược dự phòng: random, hash, hoặc default partition.
- Trong bài, ta dùng random cho key null và hash cho zone không xác định. Có thể tùy chỉnh theo yêu cầu thực tế (ví dụ: gửi vào partition 0 để dễ debug, hoặc reject message).

### d) Ứng dụng thực tế

- Hệ thống kho hàng: đảm bảo hàng hoá ở cùng khu vực được xử lý bởi cùng consumer group instance (consumer được gán partition cố định). Nhờ đó, dữ liệu liên quan được xử lý tuần tự, giảm xung đột, tận dụng cache local.
- Có thể kết hợp với consumer group để mỗi partition được xử lý bởi một instance ở khu vực địa lý tương ứng (ví dụ: service ở miền Bắc đọc partition 0 chứa NORTH và SOUTH).

### e) Hạn chế

- Số partition cố định khi tạo topic → khó mở rộng nếu thêm zone mới. Có thể dùng chiến lược hash động (consistent hashing) nhưng phức tạp hơn.
- Thay đổi logic partitioner đòi hỏi producer và consumer phải thống nhất, nếu không dữ liệu cũ có thể sai partition.
