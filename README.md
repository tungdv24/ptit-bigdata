# Bài 14: Đếm số nguyên tố với MapReduce và Spark

---

## Mô tả bài toán

Đếm số lượng các số nguyên tố có trong một file dữ liệu cho trước. File chứa nhiều số nguyên, nhiệm vụ là xác định mỗi số có phải số nguyên tố hay không, rồi đếm tổng số lượng số nguyên tố.

Số nguyên tố là số tự nhiên lớn hơn 1, chỉ chia hết cho 1 và chính nó (ví dụ: 2, 3, 5, 7, 11...).

### Input

File văn bản chứa các số nguyên, cách nhau bởi khoảng trắng hoặc xuống dòng.

Ví dụ file `numbers.txt`:

```
1 2 3 4 5 6 7 8 9 10
11 12 13 14 15 16 17 18 19 20
```

### Output mong muốn

Số lượng số nguyên tố (và không nguyên tố) tìm được:

```
NOT_PRIME    12
PRIME        8
```

Với input ví dụ trên, các số nguyên tố là: 2, 3, 5, 7, 11, 13, 17, 19 → **8 số nguyên tố**.

---

## Hadoop MapReduce

### Thiết kế hàm Map

Hàm Map đọc từng dòng dữ liệu đầu vào, tách thành các số riêng lẻ. Với mỗi số, kiểm tra tính nguyên tố:

- Nếu là số nguyên tố → phát ra cặp `(PRIME, 1)`
- Nếu không → phát ra cặp `(NOT_PRIME, 1)`

```
Input:  "2 3 4 5"
Map:
    2 → (PRIME, 1)
    3 → (PRIME, 1)
    4 → (NOT_PRIME, 1)
    5 → (PRIME, 1)
```

### Thiết kế hàm Reduce

Hàm Reduce nhận các cặp đã được gom nhóm theo key (Hadoop tự động Shuffle & Sort). Với mỗi key, cộng tổng tất cả các giá trị `1` để ra số lượng.

```
Sau Shuffle & Sort:
    PRIME     → [1, 1, 1, ...]
    NOT_PRIME → [1, 1, ...]

Reduce:
    PRIME     → sum = 8
    NOT_PRIME → sum = 12
```

### Mã nguồn Python

#### mapper.py

```python
#!/usr/bin/env python3
"""
Mapper: Đọc từng dòng, tách số, kiểm tra nguyên tố.
Output: PRIME\t1 hoặc NOT_PRIME\t1 cho mỗi số.
"""
import sys
import math

def is_prime(n):
    """Kiểm tra n có phải số nguyên tố không"""
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        if n % i == 0:
            return False
    return True

# Đọc từng dòng từ stdin (Hadoop Streaming gửi dữ liệu qua stdin)
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Tách các số trong dòng (hỗ trợ dấu cách, tab, phẩy)
    tokens = line.replace(',', ' ').split()

    for token in tokens:
        try:
            number = int(token)
            if is_prime(number):
                print("PRIME\t1")
            else:
                print("NOT_PRIME\t1")
        except ValueError:
            # Bỏ qua token không phải số
            continue
```

#### reducer.py

```python
#!/usr/bin/env python3
"""
Reducer: Đếm tổng số nguyên tố và không nguyên tố.
Input đã được Hadoop sort theo key.
Output: PRIME\t<tổng>  và  NOT_PRIME\t<tổng>
"""
import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split('\t')
    if len(parts) != 2:
        continue

    key = parts[0]
    try:
        value = int(parts[1])
    except ValueError:
        continue

    # Nếu key thay đổi → xuất kết quả của key trước đó
    if current_key and current_key != key:
        print(f"{current_key}\t{current_count}")
        current_count = 0

    current_key = key
    current_count += value

# Xuất kết quả cho key cuối cùng
if current_key:
    print(f"{current_key}\t{current_count}")
```

### Chạy chương trình

**Bước 1 — Test nhanh trên local (không cần Hadoop):**

```bash
cat numbers.txt | python3 mapper.py | sort | python3 reducer.py
```

**Bước 2 — Upload dữ liệu lên HDFS:**

```bash
hdfs dfs -mkdir -p /user/hdoop/prime/input
hdfs dfs -put -f numbers.txt /user/hdoop/prime/input/
```

**Bước 3 — Xóa output cũ (nếu có):**

```bash
hdfs dfs -rm -r /user/hdoop/prime/output 2>/dev/null
```

**Bước 4 — Chạy MapReduce job bằng Hadoop Streaming:**

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -input /user/hdoop/prime/input/numbers.txt \
  -output /user/hdoop/prime/output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py
```

**Bước 5 — Xem kết quả:**

```bash
hdfs dfs -cat /user/hdoop/prime/output/part-00000
```

Kết quả:

```
NOT_PRIME    12
PRIME        8
```

---

## Apache Spark

### Thiết kế luồng xử lý (transformations)

Khác với MapReduce chia thành 2 hàm Map/Reduce riêng biệt, Spark thiết kế theo **chuỗi biến đổi (transformations)** trên RDD, kết thúc bằng một **action** để lấy kết quả.

```
textFile        → Đọc file thành RDD các dòng
   ↓
flatMap(split)  → Tách mỗi dòng thành các số riêng lẻ
   ↓
map(int)        → Chuyển chuỗi thành số nguyên
   ↓
filter(is_prime)→ Giữ lại các số nguyên tố
   ↓
count()         → (Action) Đếm số lượng
```

| Bước | Kiểu | Tương đương MapReduce |
|------|------|----------------------|
| `flatMap` + `map` + `filter` | Transformation | Pha Map (tách số, kiểm tra nguyên tố) |
| `count` | Action | Pha Reduce (tổng hợp đếm) |

> **Lưu ý thiết kế:** Trong Spark, việc kiểm tra nguyên tố (`is_prime`) được đóng gói thành một hàm và truyền vào `filter()`. Spark tự động phân phối hàm này đến các executor để xử lý song song — tương tự cách Hadoop chạy hàm Map trên nhiều node.

### Mã nguồn Python

#### prime_spark.py

```python
#!/usr/bin/env python3
"""
Bài 14: Đếm số nguyên tố với PySpark, chạy trên YARN.
Đọc input từ HDFS (cùng file với MapReduce).
"""
from pyspark.sql import SparkSession
import math

def is_prime(n):
    """Kiểm tra n có phải số nguyên tố không"""
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        if n % i == 0:
            return False
    return True

# Khởi tạo Spark session
spark = SparkSession.builder.appName("PrimeCounter").getOrCreate()
sc = spark.sparkContext

# Đọc file input từ HDFS (đổi đường dẫn theo file của bạn)
rdd = sc.textFile("hdfs://localhost:9000/user/hdoop/prime/input/numbers.txt")

# Luồng biến đổi: tách số → chuyển int → lọc nguyên tố
numbers = rdd.flatMap(lambda line: line.replace(',', ' ').split()) \
             .map(int)

total = numbers.count()
primes = numbers.filter(is_prime).count()

# In kết quả
print("=" * 50)
print(f"Tổng số:            {total}")
print(f"Số nguyên tố:       {primes}")
print(f"Số không nguyên tố: {total - primes}")
print("=" * 50)

spark.stop()
```

### Chạy chương trình

**Đọc từ HDFS (mặc định trong code):**

```bash
spark-submit --master yarn --deploy-mode client prime_spark.py
```

**Đọc trực tiếp file local** (đổi đường dẫn trong code thành `file:///home/hdoop/tungdv/numbers.txt`):

```bash
spark-submit --master yarn --deploy-mode client prime_spark.py
```

**Chỉ hiển thị kết quả (ẩn log):**

```bash
spark-submit --master yarn --deploy-mode client prime_spark.py 2>/dev/null
```

Kết quả:

```
==================================================
Tổng số:            20
Số nguyên tố:       8
Số không nguyên tố: 12
==================================================
```

---

## So sánh Hadoop MapReduce và Spark

| Tiêu chí | Hadoop MapReduce | Spark |
|----------|------------------|-------|
| Cách thiết kế | 2 hàm riêng: Map + Reduce | Chuỗi transformations → action |
| Số file mã nguồn | 2 (mapper.py + reducer.py) | 1 (prime_spark.py) |
| Xử lý dữ liệu | Ghi/đọc đĩa giữa các pha | Xử lý in-memory (nhanh hơn) |
| Cách chạy | `hadoop jar ... streaming` | `spark-submit` |
| Xem kết quả | `hdfs dfs -cat output/part-00000` | In trực tiếp ra màn hình |
| Phù hợp | Batch processing, ETL đơn giản | Phân tích tương tác, ML, streaming |

Cả hai đều cho **cùng kết quả** (số lượng số nguyên tố giống nhau), xác nhận logic xử lý đúng ở cả hai nền tảng.

---

## Cấu trúc thư mục dự án

```text
.
├── test/                       # Bộ công cụ thực nghiệm và benchmark tự động
│   ├── run_overnight_benchmark.sh  # Kịch bản tự động hóa tuần tự 3 mốc (500MB, 1GB, 10GB)
│   ├── mapper.py               # Map function cho Hadoop Streaming
│   ├── reducer.py              # Reduce function cho Hadoop Streaming
│   ├── prime_spark.py          # PySpark script đếm số nguyên tố trong RAM
│   ├── generate_numbers.py     # Script sinh dữ liệu kiểm thử
│   ├── format_mr_output.py     # Định dạng output của MapReduce
│   ├── plot_monitor.py         # Vẽ dashboard so sánh tài nguyên & Disk I/O
│   ├── plot_scalability.py     # Vẽ biểu đồ tổng hợp Scalability & Speedup
│   └── README.md               # Hướng dẫn chi tiết chạy bộ test
├── Benchmark-Test/             # Kết quả đo đạc thực nghiệm đã hoàn thành
│   ├── 500MB/                  # Biểu đồ và logs mốc 500MB (Spark nhanh hơn 2.63x)
│   ├── 1GB/                    # Biểu đồ và logs mốc 1GB (Spark nhanh hơn 3.10x)
│   ├── 10GB/                   # Biểu đồ và logs mốc 10GB (Spark nhanh hơn 3.83x)
│   ├── MASTER_REPORT.md        # Báo cáo tổng hợp đối chiếu số liệu
│   └── scalability_chart.png   # Biểu đồ tăng tốc và thời gian thực thi
└── setup_hadoop_spark.sh       # Kịch bản cài đặt và cấu hình cụm trên Ubuntu
```

