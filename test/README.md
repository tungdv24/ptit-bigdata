# Thư Mục Test: Bộ Công Cụ Benchmark Tự Động (MapReduce vs Spark)

Thư mục này chứa toàn bộ mã nguồn thực thi, kịch bản tự động hóa và các tiện ích phục vụ bài toán so sánh hiệu năng giữa **Hadoop MapReduce** và **Apache Spark** (Bài toán đếm số nguyên tố - Prime Counting).

---

## 1. Danh sách các tệp tin và vai trò

| Tệp tin | Vai trò / Chức năng |
| :--- | :--- |
| `run_overnight_benchmark.sh` | **Kịch bản điều phối chính**: Tự động chạy tuần tự các bộ dữ liệu (`500MB` $\rightarrow$ `1GB` $\rightarrow$ `10GB`), tích hợp bộ monitor tài nguyên, thời gian nghỉ làm mát, và tự động xuất báo cáo tổng hợp. |
| `mapper.py` | Tiến trình Map (Hadoop Streaming): Đọc từng dòng dữ liệu từ stdin, kiểm tra số nguyên tố $O(\sqrt{N})$ và xuất cặp `key-value`. |
| `reducer.py` | Tiến trình Reduce (Hadoop Streaming): Tổng hợp số lượng số nguyên tố và số không nguyên tố. |
| `prime_spark.py` | Ứng dụng Apache Spark (PySpark): Phân tán dữ liệu qua RDD / `mapPartitions` để đếm số nguyên tố song song trong RAM. |
| `format_mr_output.py` | Tiện ích chuẩn hóa và hiển thị kết quả output của MapReduce. |
| `generate_numbers.py` | Tiện ích sinh tệp dữ liệu số nguyên ngẫu nhiên theo dung lượng mong muốn (ví dụ 500MB, 1GB, 10GB). |
| `plot_monitor.py` | Script vẽ biểu đồ giám sát tài nguyên: Dashboard so sánh gộp $t=0$ (`resource_chart.png`), dòng thời gian (`timeline_chart.png`), và Disk I/O (`disk_io_chart.png`). |
| `plot_scalability.py` | Script vẽ biểu đồ tổng hợp khả năng mở rộng (`scalability_chart.png`) đối chiếu Runtime và Speedup qua các mốc dữ liệu. |

---

## 2. Cách khởi chạy Benchmark

### Chạy toàn bộ 3 mốc dữ liệu (500MB -> 1GB -> 10GB):
```bash
cd test
./run_overnight_benchmark.sh
```

### Chạy kiểm thử cho 1 tệp dữ liệu duy nhất:
```bash
cd test
./run_overnight_benchmark.sh numbers_500mb.txt
```

> **Lưu ý**: Khuyến khích khởi chạy trong `tmux` hoặc `screen` trên máy chủ để tiến trình tiếp tục chạy ngầm khi ngắt kết nối SSH:
> ```bash
> tmux new -s benchmark
> cd test && ./run_overnight_benchmark.sh
> # Nhấn Ctrl+B rồi nhấn D để detach
> ```
