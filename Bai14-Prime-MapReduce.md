# Bài 14: Kiểm tra số nguyên tố với MapReduce trên Hadoop

---

## Phần 0: Hướng dẫn chạy nhanh (Quick Start)

> Phần này hướng dẫn cách chạy demo đếm số nguyên tố với file input thực tế:
> **`/home/hdoop/tungdv/numbers_test.txt`**

### 0.1 Chuẩn bị

Đảm bảo Hadoop và YARN đang chạy:

```bash
start-dfs.sh
start-yarn.sh
jps   # Kiểm tra: NameNode, DataNode, ResourceManager, NodeManager, SecondaryNameNode
```

Kiểm tra file input tồn tại trên máy local:

```bash
ls -lh /home/hdoop/tungdv/numbers_test.txt
wc -w /home/hdoop/tungdv/numbers_test.txt   # Đếm tổng số lượng số trong file
```

### 0.2 Cách 1: Chạy bằng MapReduce (Hadoop Streaming)

**Bước 1 — Upload file input lên HDFS:**

```bash
# Tạo thư mục trên HDFS
hdfs dfs -mkdir -p /user/hdoop/prime/input

# Upload file từ local lên HDFS
hdfs dfs -put -f /home/hdoop/tungdv/numbers_test.txt /user/hdoop/prime/input/

# Kiểm tra
hdfs dfs -ls /user/hdoop/prime/input/
```

**Bước 2 — Xóa thư mục output cũ (nếu có):**

```bash
hdfs dfs -rm -r /user/hdoop/prime/output 2>/dev/null
```

**Bước 3 — Chạy MapReduce job:**

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -input /user/hdoop/prime/input/numbers_test.txt \
  -output /user/hdoop/prime/output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py
```

**Bước 4 — Xem kết quả:**

```bash
hdfs dfs -cat /user/hdoop/prime/output/part-00000
```

Kết quả có dạng:

```
NOT_PRIME	<số lượng số không nguyên tố>
PRIME	<số lượng số nguyên tố>
```

### 0.3 Cách 2: Chạy bằng Spark (PySpark trên YARN)

**Cách A — Đọc trực tiếp từ file local (nhanh, không cần upload HDFS):**

```bash
spark-submit --master yarn --deploy-mode client prime_spark.py 2>&1 | grep -A5 "KẾT QUẢ"
```

> Lưu ý: sửa đường dẫn input trong `prime_spark.py` thành `file:///home/hdoop/tungdv/numbers_test.txt`
> (tiền tố `file://` để Spark đọc file trên local thay vì HDFS).

**Cách B — Đọc từ HDFS (sau khi đã upload ở bước 0.2):**

```bash
spark-submit --master yarn --deploy-mode client prime_spark.py
```

> Trong `prime_spark.py`, đường dẫn input là `hdfs://localhost:9000/user/hdoop/prime/input/numbers_test.txt`

**Cách C — Chạy nhanh interactive (pyspark shell):**

```bash
pyspark --master yarn
```

```python
import math

def is_prime(n):
    if n < 2: return False
    if n == 2: return True
    if n % 2 == 0: return False
    return all(n % i != 0 for i in range(3, int(math.sqrt(n)) + 1, 2))

# Đọc từ local
rdd = sc.textFile("file:///home/hdoop/tungdv/numbers_test.txt")
# Hoặc đọc từ HDFS:
# rdd = sc.textFile("hdfs://localhost:9000/user/hdoop/prime/input/numbers_test.txt")

numbers = rdd.flatMap(lambda l: l.split()).map(int)
total = numbers.count()
primes = numbers.filter(is_prime).count()

print(f"Tổng số: {total}")
print(f"Số nguyên tố: {primes}")
print(f"Số không nguyên tố: {total - primes}")
```

### 0.4 So sánh nhanh 2 cách

| | MapReduce (Hadoop Streaming) | Spark (PySpark) |
|--|------------------------------|-----------------|
| Bắt buộc upload HDFS | Có | Không (đọc được cả local) |
| Tốc độ | Chậm hơn | Nhanh hơn (in-memory) |
| Số file cần | mapper.py + reducer.py | 1 file prime_spark.py |
| Xem kết quả | `hdfs dfs -cat output/part-00000` | In trực tiếp ra màn hình |

> **Chi tiết mã nguồn `mapper.py`, `reducer.py`, `prime_spark.py`** xem tại [Phần 2](#phần-2-cài-đặt-và-demo) và [Phần 6](#phần-6-giải-pháp-sử-dụng-spark-pyspark).

---

## Phần 1: Lý thuyết

### 1.1 Hadoop là gì?

Hadoop là nền tảng mã nguồn mở cho phép xử lý dữ liệu lớn theo mô hình phân tán. Thay vì xử lý trên 1 máy, Hadoop chia nhỏ dữ liệu ra nhiều máy để xử lý song song.

**Các thành phần chính:**

```
┌─────────────────────────────────────────┐
│              Hadoop Ecosystem           │
├─────────────────────────────────────────┤
│  MapReduce / Spark   (Xử lý dữ liệu)  │
├─────────────────────────────────────────┤
│  YARN                (Quản lý tài nguyên)│
├─────────────────────────────────────────┤
│  HDFS                (Lưu trữ phân tán) │
└─────────────────────────────────────────┘
```

| Thành phần | Vai trò |
|------------|---------|
| **HDFS** | Hệ thống file phân tán — chia file lớn thành các block và lưu trên nhiều máy |
| **YARN** | Quản lý tài nguyên (CPU, RAM) — phân bổ cho các job |
| **MapReduce** | Mô hình lập trình — xử lý dữ liệu song song qua 2 pha: Map và Reduce |

### 1.2 MapReduce là gì?

MapReduce là mô hình lập trình gồm 2 giai đoạn:

```
Input Data → [SPLIT] → MAP → [SHUFFLE & SORT] → REDUCE → Output
```

| Giai đoạn | Nhiệm vụ |
|-----------|----------|
| **Map** | Đọc từng dòng dữ liệu, xử lý và tạo cặp (key, value) trung gian |
| **Shuffle & Sort** | Hadoop tự động nhóm các value có cùng key lại với nhau |
| **Reduce** | Nhận các nhóm (key, [values]) và tổng hợp kết quả cuối cùng |

### 1.3 Kiến trúc MapReduce cho bài toán số nguyên tố

**Bài toán:** Cho file chứa nhiều số nguyên, đếm có bao nhiêu số nguyên tố.

**Ý tưởng:**

```
┌──────────────────────────────────────────────────────────────────┐
│                        INPUT FILE                                 │
│  "2 7 10 13 4 17 9 23 15 29 6 31 8 37 20 41 11 43 50 47"       │
└───────────────────────────┬──────────────────────────────────────┘
                            │
                   ┌────────▼────────┐
                   │   SPLIT (chia)   │
                   └────────┬────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
    │   Mapper 1   │ │   Mapper 2   │ │   Mapper 3   │
    │ "2 7 10 13"  │ │ "4 17 9 23"  │ │ "15 29 6 31" │
    │              │ │              │ │              │
    │ 2→nguyên tố │ │ 4→không      │ │ 15→không     │
    │ 7→nguyên tố │ │ 17→nguyên tố │ │ 29→nguyên tố │
    │ 10→không    │ │ 9→không      │ │ 6→không      │
    │ 13→nguyên tố│ │ 23→nguyên tố │ │ 31→nguyên tố │
    └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
           │                │                │
           │  Output:       │  Output:       │  Output:
           │ (PRIME,1)x3    │ (PRIME,1)x2    │ (PRIME,1)x2
           │                │                │
           └────────────────┼────────────────┘
                            │
                   ┌────────▼────────┐
                   │  SHUFFLE & SORT  │
                   │                  │
                   │ PRIME → [1,1,1,  │
                   │   1,1,1,1,...]   │
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │    Reducer       │
                   │                  │
                   │ PRIME: sum([1,1, │
                   │  1,1,1,...]) = N │
                   └────────┬────────┘
                            │
                   ┌────────▼────────┐
                   │  OUTPUT: N số   │
                   │  nguyên tố      │
                   └─────────────────┘
```

**Giải thích thuật toán:**

1. **Map Phase:** Mỗi Mapper đọc một phần dữ liệu, tách các số ra, kiểm tra từng số có phải nguyên tố không. Nếu đúng → xuất ra cặp `("PRIME", 1)`.

2. **Shuffle & Sort:** Hadoop tự động gom tất cả các cặp có key = "PRIME" lại thành `("PRIME", [1, 1, 1, ...])`

3. **Reduce Phase:** Reducer nhận danh sách các giá trị `1` và cộng tổng lại → cho ra số lượng số nguyên tố.

### 1.4 Thuật toán kiểm tra số nguyên tố

```
isPrime(n):
    nếu n < 2: return False
    nếu n = 2: return True
    nếu n chẵn: return False
    với i từ 3 đến √n (bước 2):
        nếu n chia hết cho i: return False
    return True
```

Độ phức tạp: O(√n) cho mỗi số.

---

## Phần 2: Cài đặt và Demo

### 2.1 Cấu trúc project

```
prime-mapreduce/
├── mapper.py           # Chương trình Map
├── reducer.py          # Chương trình Reduce
├── input/
│   └── numbers.txt     # File dữ liệu đầu vào
├── run.sh              # Script chạy demo
└── README.md
```

### 2.2 Mã nguồn

#### mapper.py

```python
#!/usr/bin/env python3
"""
Mapper: Đọc từng dòng, tách số, kiểm tra nguyên tố.
Input:  Dòng text chứa các số cách nhau bởi khoảng trắng hoặc mỗi số trên 1 dòng
Output: PRIME\t1 (cho mỗi số nguyên tố tìm thấy)
        NOT_PRIME\t1 (cho mỗi số không phải nguyên tố)
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
    
    # Tách các số trong dòng (hỗ trợ cả dấu cách, tab, phẩy)
    tokens = line.replace(',', ' ').split()
    
    for token in tokens:
        try:
            number = int(token)
            if is_prime(number):
                # Xuất key=PRIME, value=1
                print(f"PRIME\t1")
            else:
                # Xuất key=NOT_PRIME, value=1
                print(f"NOT_PRIME\t1")
        except ValueError:
            # Bỏ qua token không phải số
            continue
```

#### reducer.py

```python
#!/usr/bin/env python3
"""
Reducer: Đếm tổng số nguyên tố và không nguyên tố.
Input:  key\tvalue (đã được sort theo key bởi Hadoop)
Output: PRIME\t<tổng số nguyên tố>
        NOT_PRIME\t<tổng số không nguyên tố>
"""
import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    # Tách key và value
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

#### input/numbers.txt (Dữ liệu mẫu)

```
2 3 4 5 6 7 8 9 10 11
12 13 14 15 16 17 18 19 20 21
22 23 24 25 26 27 28 29 30 31
32 33 34 35 36 37 38 39 40 41
42 43 44 45 46 47 48 49 50 51
52 53 54 55 56 57 58 59 60 61
62 63 64 65 66 67 68 69 70 71
72 73 74 75 76 77 78 79 80 81
82 83 84 85 86 87 88 89 90 91
92 93 94 95 96 97 98 99 100 101
```

> File chứa các số từ 2 đến 101. Kết quả đúng: có **26 số nguyên tố** trong khoảng [2, 101].

### 2.3 Chạy Demo

#### Cách 1: Test trên local (không cần Hadoop)

Kiểm tra logic trước khi chạy trên Hadoop:

```bash
cd prime-mapreduce

# Chạy pipeline Map → Sort → Reduce
cat input/numbers.txt | python3 mapper.py | sort | python3 reducer.py
```

Kết quả mong đợi:

```
NOT_PRIME	74
PRIME	26
```

#### Cách 2: Chạy trên Hadoop (Hadoop Streaming)

```bash
# Bước 1: Upload dữ liệu lên HDFS
hdfs dfs -mkdir -p /user/hdoop/prime/input
hdfs dfs -put input/numbers.txt /user/hdoop/prime/input/

# Bước 2: Xóa output cũ (nếu có)
hdfs dfs -rm -r /user/hdoop/prime/output 2>/dev/null

# Bước 3: Chạy MapReduce job bằng Hadoop Streaming
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -input /user/hdoop/prime/input/numbers.txt \
  -output /user/hdoop/prime/output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py
```

```bash
# Bước 4: Xem kết quả
hdfs dfs -cat /user/hdoop/prime/output/part-00000
```

Kết quả:

```
NOT_PRIME	74
PRIME	26
```

#### run.sh (Script chạy tự động)

```bash
#!/bin/bash
# Script chạy demo MapReduce đếm số nguyên tố trên Hadoop

echo "=== BÀI 14: ĐẾM SỐ NGUYÊN TỐ VỚI MAPREDUCE ==="
echo ""

# Cấu hình
INPUT_DIR="/user/hdoop/prime/input"
OUTPUT_DIR="/user/hdoop/prime/output"
INPUT_FILE="input/numbers.txt"

# Bước 1: Tạo thư mục và upload dữ liệu
echo "[1/4] Upload dữ liệu lên HDFS..."
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -put -f $INPUT_FILE $INPUT_DIR/
echo "  Done. File trên HDFS:"
hdfs dfs -ls $INPUT_DIR

echo ""

# Bước 2: Xóa output cũ
echo "[2/4] Xóa output cũ..."
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null
echo "  Done."

echo ""

# Bước 3: Chạy MapReduce
echo "[3/4] Chạy MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -input $INPUT_DIR/numbers.txt \
  -output $OUTPUT_DIR \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py

echo ""

# Bước 4: Hiển thị kết quả
echo "[4/4] Kết quả:"
echo "========================"
hdfs dfs -cat $OUTPUT_DIR/part-00000
echo "========================"
echo ""
echo "=== HOÀN TẤT ==="
```

```bash
chmod +x run.sh
./run.sh
```

---

## Phần 3: Giải thích luồng hoạt động

### 3.1 Luồng dữ liệu chi tiết

```
numbers.txt (trên HDFS)
       │
       │  HDFS chia thành các InputSplit (mặc định 128MB/block)
       │  Vì file nhỏ → chỉ 1 split
       │
       ▼
┌─────────────────────────────────────────────────┐
│  MAP PHASE                                       │
│                                                  │
│  Mapper đọc từng dòng:                           │
│  "2 3 4 5 6 7 8 9 10 11"                       │
│                                                  │
│  Với mỗi số:                                    │
│    2  → is_prime(2)  = True  → emit("PRIME", 1)│
│    3  → is_prime(3)  = True  → emit("PRIME", 1)│
│    4  → is_prime(4)  = False → emit("NOT_PRIME", 1)│
│    5  → is_prime(5)  = True  → emit("PRIME", 1)│
│    6  → is_prime(6)  = False → emit("NOT_PRIME", 1)│
│    ...                                           │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│  SHUFFLE & SORT (Hadoop tự thực hiện)           │
│                                                  │
│  Gom theo key:                                   │
│  "NOT_PRIME" → [1, 1, 1, ..., 1]  (74 phần tử)│
│  "PRIME"     → [1, 1, 1, ..., 1]  (26 phần tử)│
└────────────────────────┬────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│  REDUCE PHASE                                    │
│                                                  │
│  Reducer nhận:                                   │
│  "NOT_PRIME" [1,1,...,1] → sum = 74             │
│  "PRIME"     [1,1,...,1] → sum = 26             │
│                                                  │
│  Output:                                         │
│  NOT_PRIME    74                                 │
│  PRIME        26                                 │
└─────────────────────────────────────────────────┘
```

### 3.2 Tại sao dùng MapReduce?

| Vấn đề | Cách truyền thống | Cách MapReduce |
|--------|-------------------|----------------|
| File 1 GB số | 1 máy xử lý tuần tự → chậm | Chia cho N máy xử lý song song → nhanh gấp N lần |
| Máy bị lỗi | Mất kết quả, phải chạy lại | Hadoop tự chạy lại task bị lỗi trên máy khác |
| Mở rộng | Phải viết lại code | Thêm máy → tự động nhanh hơn |

### 3.3 Hadoop Streaming

Hadoop Streaming cho phép viết Mapper và Reducer bằng bất kỳ ngôn ngữ nào (Python, Ruby, C++...) thay vì chỉ Java.

Quy ước:
- **Mapper:** đọc từ `stdin`, xuất ra `stdout` theo format `key\tvalue`
- **Reducer:** đọc từ `stdin` (đã được sort theo key), xuất ra `stdout`

---

## Phần 4: Kết quả Demo

### 4.1 Kết quả trên local

```bash
$ cat input/numbers.txt | python3 mapper.py | sort | python3 reducer.py
NOT_PRIME	74
PRIME	26
```

### 4.2 Kết quả trên Hadoop

```bash
$ hdfs dfs -cat /user/hdoop/prime/output/part-00000
NOT_PRIME	74
PRIME	26
```

### 4.3 Kiểm chứng

Các số nguyên tố trong khoảng [2, 101]:

```
2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47,
53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101
```

Tổng: **26 số** → khớp với kết quả MapReduce.

### 4.4 Screenshots (cần bổ sung)

- [ ] Screenshot HDFS Web UI (http://localhost:9870) hiển thị file input
- [ ] Screenshot terminal khi chạy MapReduce job
- [ ] Screenshot YARN Web UI (http://localhost:8088) hiển thị job SUCCEEDED
- [ ] Screenshot kết quả output

---

## Phần 5: Phân công nhóm

| STT | Thành viên | Công việc | Tỷ lệ |
|-----|-----------|-----------|--------|
| 1 | ... | Tìm hiểu lý thuyết Hadoop, MapReduce. Viết báo cáo phần lý thuyết | ...% |
| 2 | ... | Cài đặt Hadoop Standalone, viết code mapper.py / reducer.py | ...% |
| 3 | ... | Tạo dữ liệu mẫu, chạy demo, chụp screenshot kết quả | ...% |
| 4 | ... | Tổng hợp báo cáo, chuẩn bị slide thuyết trình | ...% |

---

## Tài liệu tham khảo

1. Apache Hadoop Documentation - https://hadoop.apache.org/docs/r3.3.6/
2. Hadoop Streaming - https://hadoop.apache.org/docs/r3.3.6/hadoop-streaming/HadoopStreaming.html
3. MapReduce Tutorial - https://hadoop.apache.org/docs/r3.3.6/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html


---

## Phần 6: Giải pháp sử dụng Spark (PySpark)

### 6.1 Tại sao dùng Spark?

| So sánh | MapReduce (Hadoop Streaming) | Spark |
|---------|------------------------------|-------|
| Tốc độ | Chậm (đọc/ghi đĩa giữa các phase) | Nhanh 10-100x (xử lý in-memory) |
| Code | Phải viết riêng mapper.py + reducer.py | 1 file duy nhất, ngắn gọn |
| API | Thấp (stdin/stdout) | Cao (DataFrame, RDD, SQL) |
| Interactive | Không | Có (pyspark shell) |

### 6.2 Mã nguồn PySpark

#### prime_spark.py

```python
#!/usr/bin/env python3
"""
Bài 14: Đếm số nguyên tố với PySpark
Chạy trên YARN hoặc local mode
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

# Tạo Spark session
spark = SparkSession.builder \
    .appName("PrimeCounter") \
    .getOrCreate()

sc = spark.sparkContext

# ============ CÁCH 1: Dùng RDD (tương tự MapReduce) ============

# Đọc file từ HDFS
rdd = sc.textFile("hdfs://localhost:9000/user/hdoop/prime/input/numbers.txt")

# Map: tách số, kiểm tra nguyên tố
# Reduce: đếm tổng
prime_count = rdd \
    .flatMap(lambda line: line.strip().replace(',', ' ').split()) \
    .map(lambda token: int(token)) \
    .filter(lambda n: is_prime(n)) \
    .count()

total_count = rdd \
    .flatMap(lambda line: line.strip().replace(',', ' ').split()) \
    .map(lambda token: int(token)) \
    .count()

print("=" * 50)
print(f"Tổng số: {total_count}")
print(f"Số nguyên tố: {prime_count}")
print(f"Số không nguyên tố: {total_count - prime_count}")
print("=" * 50)

# ============ CÁCH 2: Dùng DataFrame + UDF ============

from pyspark.sql.functions import udf, explode, split, col
from pyspark.sql.types import BooleanType, IntegerType

# Đọc file thành DataFrame
df = spark.read.text("hdfs://localhost:9000/user/hdoop/prime/input/numbers.txt")

# Tách các số ra từng dòng
numbers_df = df.select(
    explode(split(col("value"), "\\s+")).alias("num_str")
).select(
    col("num_str").cast(IntegerType()).alias("number")
).filter(col("number").isNotNull())

# Đăng ký UDF kiểm tra nguyên tố
is_prime_udf = udf(is_prime, BooleanType())

# Thêm cột is_prime
result_df = numbers_df.withColumn("is_prime", is_prime_udf(col("number")))

# Đếm
prime_df = result_df.filter(col("is_prime") == True)

print("\n=== KẾT QUẢ (DataFrame API) ===")
print(f"Số nguyên tố: {prime_df.count()}")
print("\nDanh sách số nguyên tố:")
prime_df.select("number").orderBy("number").show(50, truncate=False)

# Dừng Spark
spark.stop()
```

### 6.3 Chạy trên Hadoop YARN

```bash
# Đảm bảo file input đã có trên HDFS
hdfs dfs -ls /user/hdoop/prime/input/numbers.txt

# Chạy PySpark trên YARN
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --num-executors 1 \
  prime_spark.py
```

### 6.4 Chạy local (không cần Hadoop)

```bash
# Chạy với local mode (dùng file trên máy local)
spark-submit --master local[*] prime_spark_local.py
```

#### prime_spark_local.py (phiên bản chạy local)

```python
#!/usr/bin/env python3
"""Phiên bản chạy local - không cần HDFS"""
from pyspark.sql import SparkSession
import math

def is_prime(n):
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

spark = SparkSession.builder \
    .appName("PrimeCounter-Local") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Đọc file local
rdd = sc.textFile("input/numbers.txt")

# Tách số → lọc nguyên tố → đếm
numbers = rdd.flatMap(lambda line: line.split()).map(int)
primes = numbers.filter(is_prime)

print(f"\nTổng số: {numbers.count()}")
print(f"Số nguyên tố: {primes.count()}")
print(f"Danh sách: {sorted(primes.collect())}")

spark.stop()
```

```bash
spark-submit --master local[*] prime_spark_local.py
```

Kết quả:

```
Tổng số: 100
Số nguyên tố: 26
Danh sách: [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101]
```

### 6.5 Chạy Interactive (PySpark Shell)

```bash
pyspark --master yarn
```

```python
# Trong PySpark shell - đếm nhanh trong vài dòng
import math

def is_prime(n):
    if n < 2: return False
    if n == 2: return True
    if n % 2 == 0: return False
    return all(n % i != 0 for i in range(3, int(math.sqrt(n)) + 1, 2))

rdd = sc.textFile("/user/hdoop/prime/input/numbers.txt")
primes = rdd.flatMap(lambda l: l.split()).map(int).filter(is_prime)
print(f"Số nguyên tố: {primes.count()}")
# Kết quả: Số nguyên tố: 26
```

### 6.6 So sánh MapReduce vs Spark cho bài toán này

| Tiêu chí | Hadoop MapReduce | Spark |
|-----------|-----------------|-------|
| Số file code | 2 (mapper.py + reducer.py) | 1 (prime_spark.py) |
| Số dòng code | ~50 dòng | ~15 dòng (RDD) |
| Cách chạy | hadoop jar ... streaming | spark-submit |
| Kết quả | Chỉ ra tổng đếm | Có thể in luôn danh sách số nguyên tố |
| Debug | Khó (phải đọc log YARN) | Dễ (chạy interactive trong pyspark shell) |
| Phù hợp | Batch processing lớn, ETL | Phân tích tương tác, ML, streaming |
