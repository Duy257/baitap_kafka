Bài 2: Consumer Group và Partition Rebalancing

Mục tiêu: Hiểu rõ cách Kafka phân chia công việc giữa các Consumer.

Yêu cầu: Tạo topic orders có 6 partitions. Khởi chạy 1 consumer (group id order-group), gửi 1000 message vào. Quan sát phân phối. Sau đó, khởi chạy consumer thứ 2 và thứ 3 cùng group id. In ra log để thấy partition nào đang được assign cho consumer nào.

Thử thách: Thực hiện rebalance bằng cách tắt 1 consumer đột ngột (kill -9) và quan sát các consumer còn lại tự động chiếm partitions bị mất

Mục tiêu bài học
Hiểu cách Kafka gán partition cho từng consumer trong một consumer group.

Quan sát sự thay đổi phân công khi số lượng consumer thay đổi (tăng/giảm).

Nắm được cơ chế Rebalancing: khi một consumer bị tắt đột ngột, các consumer còn lại sẽ tự động chiếm các partition bị mất (không mất dữ liệu, nhưng có thể xảy ra tạm dừng xử lý).
