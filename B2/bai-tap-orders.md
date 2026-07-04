# Bài tập Kafka: Topic Orders — Ghi chú

## Mục tiêu

Làm quen với các thao tác quản lý topic trên Kafka CLI qua Docker.

---

## Các thao tác đã thực hiện

### 1. Tạo topic với nhiều partitions

```bash
docker exec <container> /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic orders \
  --partitions 6 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

- **partitions**: chia nhỏ topic thành nhiều shard, cho phép **parallel consume** và **scale ngang**.
- **replication-factor**: số bản sao dữ liệu (1 = không có replica, chỉ dùng cho dev/test).

> Nếu topic đã tồn tại, lỗi `TopicExistsException` sẽ xuất hiện. Lúc đó dùng `--alter` thay vì `--create`.

### 2. Alter số partitions (tăng từ 1 → 6)

```bash
kafka-topics.sh --alter --topic orders --partitions 6 --bootstrap-server localhost:9092
```

- **Có thể tăng partitions**, nhưng **không thể giảm** — đây là ràng buộc thiết kế của Kafka.
- Khi tăng partitions, dữ liệu cũ giữ nguyên, key-based ordering bị phá vỡ nếu producer dùng key.
- Kiểm tra lại bằng `--describe`:

```bash
kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

Output mẫu:

```
Topic: orders     PartitionCount: 6     ReplicationFactor: 1
    Topic: orders     Partition: 0     Leader: 1   Replicas: 1   Isr: 1
    Topic: orders     Partition: 1     Leader: 1   Replicas: 1   Isr: 1
    ...
```

### 3. Xoá toàn bộ messages trong một partition

```bash
# Bước 1: Lấy latest offset
kafka-get-offsets.sh --topic orders --partitions 0 --bootstrap-server localhost:9092
# Output: orders:0:3000  (nghĩa là offset 0 → 2999 đã có messages)

# Bước 2: Tạo JSON file
{
  "partitions": [
    { "topic": "orders", "partition": 0, "offset": 3000 }
  ],
  "version": 1
}

# Bước 3: Xoá
kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file /tmp/delete-records.json
# Output: partition: orders-0  low_watermark: 3000
```

- `low_watermark = 3000` nghĩa là tất cả messages **dưới offset 3000** đã bị xoá.
- Kafka không xoá từng message một; nó đánh dấu toàn bộ segment files chứa các offset đó để清理 (cleanup) ở tầng log.
- **Chỉ xoá được theo partition cụ thể**, không có lệnh "xoá toàn bộ topic" kiểu `TRUNCATE`.

---

## Những khái niệm quan trọng

| Khái niệm | Giải thích |
|-----------|-----------|
| **Partition** | Đơn vị song song nhỏ nhất trong Kafka. Mỗi partition là một ordered, immutable log. |
| **Offset** | Số thứ tự của message trong partition (bắt đầu từ 0). Không thể thay đổi. |
| **Watermark** | `Low watermark` = offset thấp nhất còn tồn tại; `High watermark` = offset cuối cùng đã commit. |
| **Replication Factor** | Số bản copy của dữ liệu để chịu lỗi. RF=1 là mặc định cho dev. |
| **Segment** | File vật lý trên ổ cứng chứa messages. Kafka cleanup hoạt động ở mức segment. |

---

## Lưu ý thực tế

- **Topics trong container** — tất cả commands trên đều chạy qua `docker exec`. Nếu restart container không gắn volume, dữ liệu và config topic có thể mất.
- **Không thể giảm partitions** — cần tính toán số partitions ngay từ đầu. Rule of thumb: partitions >= số consumer trong cùng một group.
- **Delete records ≠ reset** — offset không reset về 0, chỉ tăng low watermark lên. Nếu muốn reset hoàn toàn, phải xoá topic và tạo lại.
- **Xoá messages theo partition** — tiện lợi khi chỉ muốn dọn sạch 1 partition (ví dụ partition bị lỗi dữ liệu), không ảnh hưởng các partition khác.

---

## Kết luận

Bài tập này giúp làm quen với:

1. Cách tạo topic và chọn số partitions phù hợp
2. Cách alter topic (tăng partitions) — và giới hạn của nó
3. Cách xoá messages ở mức partition bằng `kafka-delete-records.sh`
4. Khái niệm offset, watermark và segment trong Kafka
5. Thao tác Kafka CLI qua Docker container
