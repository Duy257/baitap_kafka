Bài 3: Message Key và Đảm bảo thứ tự

Mục tiêu: Hiểu cơ chế phân chia partition dựa trên Key.

Yêu cầu: Topic payment có 3 partitions. Producer gửi 50 message với key là user_id (chuỗi) và value là transaction. Viết Consumer in ra partition id và giá trị.

Kiểm tra: Chứng minh rằng tất cả message có cùng user_id luôn nằm trong cùng 1 partition và được đọc đúng thứ tự (nếu bạn set max.in.flight.requests.per.
connection=1).

Kiến thức nền tảng
Message Key và Partition:
Khi Producer gửi message có key, Kafka dùng hàm băm murmur2(key) % số_partition để quyết định partition. Các message có cùng key sẽ luôn vào cùng một partition.

Đảm bảo thứ tự:
Trong một partition, message được lưu theo thứ tự gửi đến (offset tăng dần). Để duy trì thứ tự nghiêm ngặt khi gửi có key, Producer cần cấu hình max.in.flight.requests.per.connection=1 (chỉ cho phép 1 request chưa được xác nhận tại một thời điểm) và bật enable.idempotence=true (tránh trùng lặp). Nếu không, việc gửi lại khi lỗi có thể làm đảo lộn thứ tự.

Consumer:
Consumer đọc message từ partition và có thể in ra partition id, offset và nội dung. Trong bài này chúng ta chỉ cần in partition và value để chứng minh tính chất trên

3. Tạo topic payment với 3 partitions
   kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic payment \
    --partitions 3 --replication-factor 1

4. Producer code – gửi 50 message với key là user_id
   Yêu cầu cấu hình đặc biệt để đảm bảo thứ tự:

Producer.RequiredAcks = sarama.WaitForAll (ack từ tất cả replica) – tăng độ bền.

Producer.Idempotent = true (chống trùng lặp, tự động đặt max.in.flight.requests.per.connection=5 nhưng chúng ta ghi đè).

Producer.MaxInFlightRequests = 1 (quan trọng nhất cho thứ tự).

Producer.Retry.Max = 5 (số lần thử lại).

6. Kiểm chứng
   Cùng user_id → cùng partition:
   Lọc theo user_0, tất cả message của user_0 phải có cùng một số partition (ví dụ partition 0, 1 hoặc 2). Tương tự với các user khác. Có thể kiểm tra bằng mắt hoặc viết script nhỏ phân tích log.

Thứ tự đúng:
Với một user, offset tăng dần và thứ tự nội dung đúng (transaction-0, transaction-1,...). Nhờ max.in.flight.requests.per.connection=1, thứ tự gửi được bảo toàn ngay cả khi có retry. Nếu tắt idempotent và đặt max.in.flight.requests.per.connection=5, thứ tự có thể bị đảo lộn khi có lỗi mạng.

Thử nghiệm bổ sung (tùy chọn):

Thay đổi MaxInFlightRequests thành 5, tạm thời ngắt kết nối broker, bạn có thể thấy một vài message bị đảo thứ tự trong cùng partition.

Gửi không có key: message sẽ phân bố ngẫu nhiên vào các partition.

7. Những điều học được từ bài tập
   Cơ chế phân mảnh theo key: Hiểu rằng key quyết định partition, giúp nhóm các sự kiện liên quan (cùng user, cùng đơn hàng) để xử lý tuần tự.

Đảm bảo thứ tự trong Kafka: Chỉ được đảm bảo trong một partition. Muốn toàn cục (global order) thì chỉ có 1 partition, hoặc dùng cơ chế khác (ví dụ id thời gian).

Cấu hình Producer để giữ thứ tự: max.in.flight.requests.per.connection=1 là chìa khóa. Khi kết hợp với enable.idempotence=true, vừa giữ thứ tự vừa tránh trùng lặp khi retry.

Offset thể hiện thứ tự: Trong partition, offset đơn điệu tăng, cho biết thứ tự lưu trữ. Dù consumer đọc song song nhiều partition, thứ tự tổng thể không đảm bảo nhưng trên từng partition thì có.

Ứng dụng thực tế: Khi cần xử lý các sự kiện của một user theo đúng trình tự (ví dụ nạp tiền rồi mới trừ tiền), ta dùng user_id làm key và đảm bảo producer gửi đúng thứ tự + consumer xử lý tuần tự trên partition (có thể dùng single-thread per partition).

Hiểu về idempotent producer: Ngoài thứ tự, idempotent còn giúp loại bỏ trùng lặp do retry, đảm bảo dữ liệu nhất quán.
