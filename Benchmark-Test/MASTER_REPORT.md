# Báo cáo Thực nghiệm So sánh Hiệu năng: Hadoop MapReduce vs Apache Spark

**Thời điểm thực hiện**: 2026-10-05 12:12:23

## 1. Cấu hình phần cứng và phân bổ tài nguyên
- **Môi trường**: Ubuntu Linux (8 vCPUs, 16GB RAM).
- **Nguyên tắc công bằng (1:1 Resource Parity)**: Cả MapReduce và Spark đều bị khống chế ở mức **tối đa 8GB RAM và 4 vCores tính toán song song**.
  - **MapReduce**: 4 Map Containers (1.5GB / 1 vCore mỗi map) + 1 AM (2GB / 1 vCore) = 8GB RAM / 5 vCores.
  - **Spark**: 1 Executor (6GB gồm 5GB heap + 1GB overhead, 4 vCores) + 1 Driver (2GB ngoài client) = 8GB RAM / 5 vCores.

## 2. Bảng tổng hợp kết quả thực nghiệm

| Dataset | Dung lượng | Số nguyên tố | Runtime MapReduce | Runtime Spark | Tốc độ Spark (Speedup) | Đối chiếu kết quả |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **500MB** | 500 MB | 5973435 | **300s** | **114s** | **2.63x** | ✓ MATCHED |
| **1GB** | 1024 MB | 12240116 | **614s** | **198s** | **3.10x** | ✓ MATCHED |
| **10GB** | 10240 MB | 122357036 | **6688s** | **1744s** | **3.83x** | ✓ MATCHED |

## 3. Phân tích kết quả theo các trục so sánh
1. **Hiệu năng & Khả năng mở rộng (Scalability)**: Spark vượt trội MapReduce từ file nhỏ đến file lớn, đặc biệt khi dữ liệu tăng thì khoảng cách tốc độ càng được nới rộng.
2. **Hành vi CPU & I/O**: Xem biểu đồ `resource_chart.png` trong từng thư mục con để thấy rõ giai đoạn nghẽn I/O Disk Spill của MapReduce so với pipeline liên tục trong RAM của Spark.
3. **Tính đúng đắn (Correctness)**: 100% số lượng số nguyên tố đếm được khớp nhau tuyệt đối giữa hai nền tảng.
