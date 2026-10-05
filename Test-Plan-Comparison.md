# Kế hoạch Test & So sánh: MapReduce vs Spark

> Đo và so sánh thời gian chạy bài toán đếm số nguyên tố giữa Hadoop MapReduce và
> Apache Spark trên các mức dữ liệu khác nhau, đảm bảo cùng cấu hình tài nguyên
> (8GB RAM / 4 vCores) và cùng kết quả.

---

## 1. Bộ dữ liệu test

| File | Dung lượng | Mục đích |
|------|-----------|----------|
| `numbers_500mb.txt` | 500 MB | Dữ liệu nhỏ — overhead khởi động ảnh hưởng nhiều |
| `numbers_1gb.txt` | 1 GB | Dữ liệu trung bình |
| `numbers_10gb.txt` | 10 GB | Dữ liệu lớn — thể hiện rõ ưu thế xử lý in-memory của Spark |

> **3 mốc này đủ** để thấy xu hướng: file càng lớn, chênh lệch tốc độ giữa 2 framework
> càng rõ. Có thể vẽ biểu đồ đường (dung lượng → thời gian) để minh họa trong báo cáo.

---

## 2. Quy trình test (5 bước)

```
Bước 1: Tạo file dữ liệu (local)
   ↓
Bước 2: Upload lên HDFS + kiểm tra dung lượng thực trên HDFS
   ↓
Bước 3: Chạy MapReduce (ghi start/end/runtime)
   ↓
Bước 4: Chạy Spark (ghi start/end/runtime)
   ↓
Bước 5: So sánh kết quả (phải giống nhau) + so sánh thời gian
```

---

## 3. Bước 1: Tạo file dữ liệu

Dùng script `generate_numbers.py` đã có:

```bash
cd /home/hdoop/tungdv

python3 generate_numbers.py 500MB numbers_500mb.txt
python3 generate_numbers.py 1GB   numbers_1gb.txt
python3 generate_numbers.py 10GB  numbers_10gb.txt
```

Kiểm tra file trên local:

```bash
ls -lh numbers_*.txt
```

---

## 4. Bước 2: Upload lên HDFS & kiểm tra dung lượng thực

```bash
# Tạo thư mục input trên HDFS
hdfs dfs -mkdir -p /user/hdoop/prime/input

# Upload cả 3 file
hdfs dfs -put -f numbers_500mb.txt /user/hdoop/prime/input/
hdfs dfs -put -f numbers_1gb.txt   /user/hdoop/prime/input/
hdfs dfs -put -f numbers_10gb.txt  /user/hdoop/prime/input/
```

**Kiểm tra dung lượng thực tế đã lưu trên HDFS:**

```bash
# Xem kích thước từng file (cột đầu = dung lượng thực, cột 2 = tổng gồm bản sao)
hdfs dfs -du -h /user/hdoop/prime/input/

# Xem chi tiết số block của file 10GB (HDFS chia block 128MB)
hdfs fsck /user/hdoop/prime/input/numbers_10gb.txt -files -blocks 2>/dev/null | grep -E "Total|blocks"
```

> Với `dfs.replication=1`, dung lượng trên HDFS ≈ dung lượng file gốc. File 10GB sẽ
> được chia thành ~80 block (128MB/block), tức Spark/MapReduce sẽ tạo ~80 task xử lý song song.

---

## 5. Bước 3 & 4: Script tự động chạy test và đo thời gian

Tạo file `run_benchmark.sh`:

```bash
#!/bin/bash
###############################################################################
# Chạy benchmark MapReduce vs Spark trên 1 file input, ghi lại thời gian.
# HIỂN THỊ tiến độ chạy của cả 2 framework ngay trên màn hình.
#
# Cách dùng:  bash run_benchmark.sh <ten_file_tren_hdfs>
# Ví dụ:      bash run_benchmark.sh numbers_1gb.txt
###############################################################################

INPUT_FILE="$1"
if [ -z "$INPUT_FILE" ]; then
    echo "Cách dùng: bash run_benchmark.sh <ten_file>"
    echo "Ví dụ:     bash run_benchmark.sh numbers_1gb.txt"
    exit 1
fi

HDFS_INPUT="/user/hdoop/prime/input/$INPUT_FILE"
RESULT_LOG="benchmark_result_${INPUT_FILE}.log"
PHASES_FILE="phases_${INPUT_FILE}.csv"      # Ghi mốc thời gian các pha (cho vẽ biểu đồ)
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"

# Khởi tạo file mốc thời gian (epoch giây) để plot_monitor.py tô vùng MR/Spark
echo "phase,start_epoch,end_epoch" > "$PHASES_FILE"

echo "==================================================" | tee "$RESULT_LOG"
echo "BENCHMARK: $INPUT_FILE" | tee -a "$RESULT_LOG"
echo "Thời điểm bắt đầu: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$RESULT_LOG"

# Dung lượng thực trên HDFS
echo "" | tee -a "$RESULT_LOG"
echo "--- Dung lượng trên HDFS ---" | tee -a "$RESULT_LOG"
hdfs dfs -du -h "$HDFS_INPUT" | tee -a "$RESULT_LOG"

# ==================== MAPREDUCE ====================
echo "" | tee -a "$RESULT_LOG"
echo "========== MAPREDUCE (đang chạy, xem tiến độ bên dưới) ==========" | tee -a "$RESULT_LOG"
hdfs dfs -rm -r /user/hdoop/prime/output_mr 2>/dev/null

MR_START=$(date +%s.%N)
MR_START_HUMAN=$(date '+%H:%M:%S')
echo ">>> MapReduce Start: $MR_START_HUMAN"

# Ghi log ra file ĐỒNG THỜI hiện toàn bộ trên màn hình.
# Dùng tee (KHÔNG grep) để tiến độ (kể cả ký tự \r cập nhật dòng) hiện realtime.
# MapReduce in tiến độ ra stderr → gộp 2>&1 rồi đưa vào tee.
hadoop jar "$STREAMING_JAR" \
  -input "$HDFS_INPUT" \
  -output /user/hdoop/prime/output_mr \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py 2>&1 | tee mr_progress.log

MR_END=$(date +%s.%N)
MR_END_HUMAN=$(date '+%H:%M:%S')
MR_RUNTIME=$(echo "$MR_END - $MR_START" | bc)

# Ghi mốc thời gian pha MapReduce (epoch giây, làm tròn) để vẽ biểu đồ
echo "MapReduce,${MR_START%.*},${MR_END%.*}" >> "$PHASES_FILE"

# Đọc TẤT CẢ part-* (phòng khi có nhiều reducer) rồi format sang tiếng Việt
MR_RESULT=$(hdfs dfs -cat /user/hdoop/prime/output_mr/part-* 2>/dev/null | python3 format_mr_output.py)

{
  echo ">>> MapReduce End:  $MR_END_HUMAN"
  echo "Start time:  $MR_START_HUMAN"
  echo "End time:    $MR_END_HUMAN"
  echo "Runtime:     ${MR_RUNTIME} giây"
  echo "--- KẾT QUẢ MAPREDUCE ---"
  echo "$MR_RESULT"
} | tee -a "$RESULT_LOG"

# ==================== SPARK ====================
echo "" | tee -a "$RESULT_LOG"
echo "========== SPARK (đang chạy, xem tiến độ bên dưới) ==========" | tee -a "$RESULT_LOG"

SPARK_START=$(date +%s.%N)
SPARK_START_HUMAN=$(date '+%H:%M:%S')
echo ">>> Spark Start: $SPARK_START_HUMAN"

# Spark in tiến độ [Stage X:===> (a+b)/n] ra stderr (dùng \r cập nhật dòng).
# Dùng tee (KHÔNG grep) để thanh tiến độ hiện realtime trên màn hình.
spark-submit --master yarn --deploy-mode client \
  prime_spark.py "$HDFS_INPUT" 2>&1 | tee spark_progress.log

SPARK_END=$(date +%s.%N)
SPARK_END_HUMAN=$(date '+%H:%M:%S')
SPARK_RUNTIME=$(echo "$SPARK_END - $SPARK_START" | bc)

# Ghi mốc thời gian pha Spark
echo "Spark,${SPARK_START%.*},${SPARK_END%.*}" >> "$PHASES_FILE"

SPARK_RESULT=$(grep -E "Tổng số|Số nguyên tố|Số không nguyên tố" spark_progress.log)

{
  echo ">>> Spark End:  $SPARK_END_HUMAN"
  echo "Start time:  $SPARK_START_HUMAN"
  echo "End time:    $SPARK_END_HUMAN"
  echo "Runtime:     ${SPARK_RUNTIME} giây"
  echo "--- KẾT QUẢ SPARK ---"
  echo "$SPARK_RESULT"
} | tee -a "$RESULT_LOG"

# ==================== SO SÁNH ====================
# Trích số nguyên tố của mỗi bên để đối chiếu kết quả
MR_PRIME=$(echo "$MR_RESULT" | grep "Số nguyên tố" | grep -oE '[0-9]+')
SPARK_PRIME=$(echo "$SPARK_RESULT" | grep "Số nguyên tố" | grep -oE '[0-9]+')

{
  echo ""
  echo "=================================================="
  echo "                  TỔNG KẾT"
  echo "=================================================="
  echo "File:            $INPUT_FILE"
  echo ""
  echo "----- OUTPUT MAPREDUCE -----"
  echo "$MR_RESULT"
  echo ""
  echo "----- OUTPUT SPARK -----"
  echo "$SPARK_RESULT"
  echo ""
  echo "----- SO SÁNH THỜI GIAN -----"
  echo "MapReduce:       ${MR_RUNTIME} giây"
  echo "Spark:           ${SPARK_RUNTIME} giây"

  # Tính Spark nhanh hơn bao nhiêu lần
  SPEEDUP=$(echo "scale=2; $MR_RUNTIME / $SPARK_RUNTIME" | bc)
  echo "Spark nhanh hơn: ${SPEEDUP}x"

  echo ""
  echo "----- KIỂM TRA KẾT QUẢ -----"
  if [ "$MR_PRIME" = "$SPARK_PRIME" ] && [ -n "$MR_PRIME" ]; then
    echo "✓ KHỚP: Cả hai đều đếm được $MR_PRIME số nguyên tố."
  else
    echo "✗ KHÁC NHAU! MapReduce=$MR_PRIME, Spark=$SPARK_PRIME → cần kiểm tra lại!"
  fi
  echo "=================================================="
} | tee -a "$RESULT_LOG"

echo ""
echo "Kết quả đã lưu vào: $RESULT_LOG"
echo "Log tiến độ đầy đủ: mr_progress.log (MapReduce), spark_progress.log (Spark)"
```

### Giải thích cách hiển thị tiến độ

| Kỹ thuật | Tác dụng |
|----------|----------|
| `2>&1` | Gộp stderr vào stdout (tiến độ được in ra stderr) |
| `\| tee file.log` | Ghi ra file log ĐỒNG THỜI vẫn hiện toàn bộ trên màn hình |
| `mr_progress.log` / `spark_progress.log` | Giữ log đầy đủ để xem lại / trích kết quả |

> **QUAN TRỌNG — vì sao lúc dùng grep không thấy tiến độ?**
> MapReduce và Spark in thanh tiến độ bằng ký tự **`\r` (carriage return)** — cập nhật
> đè lên cùng một dòng, KHÔNG xuống dòng mới. `grep` xử lý theo dòng (`\n`) nên không
> bắt được `\r` → terminal không hiện gì dù file log vẫn đầy đủ.
>
> **Giải pháp:** bỏ `grep`, chỉ dùng `tee`. `tee` chuyển thẳng mọi byte (kể cả `\r`)
> ra terminal ngay → bạn thấy thanh tiến độ chạy sống động như chạy lệnh trực tiếp,
> đồng thời vẫn lưu đầy đủ vào file log.

**Tiến độ MapReduce** hiển thị dạng (cập nhật liên tục trên cùng dòng):
```
map 0% reduce 0%
map 45% reduce 0%
map 100% reduce 0%
map 100% reduce 100%
```

**Tiến độ Spark** hiển thị dạng:
```
[Stage 0:=========>            (25 + 4) / 80]
[Stage 0:==================>   (60 + 4) / 80]
```

> Nếu muốn xem tiến độ Spark trực quan hơn nữa (thanh %, DAG, thời gian từng task),
> mở Spark UI trong lúc job đang chạy: `http://localhost:4040`

### File cần có trong cùng thư mục

Script `run_benchmark.sh` cần các file sau nằm cùng thư mục:

| File | Vai trò |
|------|---------|
| `mapper.py` | Hàm Map (kiểm tra nguyên tố) |
| `reducer.py` | Hàm Reduce (đếm tổng, xuất `PRIME/NOT_PRIME`) |
| `format_mr_output.py` | Đọc part-* của MapReduce → in tiếng Việt giống Spark |
| `prime_spark.py` | Code Spark, **nhận đường dẫn input từ tham số** |

`prime_spark.py` phải đọc đường dẫn từ dòng lệnh (không hard-code) để chạy đúng cùng file với MapReduce:

```python
import sys
input_path = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/user/hdoop/prime/input/numbers_500mb.txt"
rdd = sc.textFile(input_path)
```

---

## 6. Bước 5: Chạy toàn bộ test

Cài `bc` nếu chưa có (để tính toán số thực):

```bash
sudo apt install bc -y
```

Chạy benchmark cho từng file:

```bash
cd /home/hdoop/tungdv

bash run_benchmark.sh numbers_500mb.txt
bash run_benchmark.sh numbers_1gb.txt
bash run_benchmark.sh numbers_10gb.txt
```

Mỗi lần chạy tạo ra 1 file log: `benchmark_result_numbers_500mb.txt.log`...

---

## 7. Bảng tổng hợp kết quả (điền sau khi chạy)

| File | Dung lượng HDFS | Số nguyên tố | MapReduce (giây) | Spark (giây) | Spark nhanh hơn |
|------|-----------------|--------------|------------------|--------------|-----------------|
| numbers_500mb.txt | ... | ... | ... | ... | ...x |
| numbers_1gb.txt | ... | ... | ... | ... | ...x |
| numbers_10gb.txt | ... | ... | ... | ... | ...x |

> **Quan trọng:** Cột "Số nguyên tố" của MapReduce và Spark trên cùng 1 file **phải giống nhau**.
> Nếu khác → có lỗi logic, cần kiểm tra lại.

---

## 8. Lưu ý để kết quả chính xác

1. **Chạy mỗi test 2-3 lần**, lấy trung bình (lần đầu chậm do JVM/cache lạnh).
2. **Không chạy MapReduce và Spark cùng lúc** — sẽ tranh tài nguyên YARN.
3. **Cùng cấu hình tài nguyên** (8GB / 4 vCores) — đã set trong `Config-Hadoop-Spark.md`.
4. **Kiểm tra kết quả giống nhau** trước khi so sánh thời gian.
5. Với file 10GB, đảm bảo đủ dung lượng đĩa trên HDFS:
   ```bash
   hdfs dfsadmin -report | grep -E "Configured Capacity|DFS Remaining"
   ```
6. Nếu job Spark treo ở `ACCEPTED` → thiếu RAM, xem lại `yarn.nodemanager.resource.memory-mb`.

---

## 9. Vẽ biểu đồ so sánh (tùy chọn, cho báo cáo đẹp)

Sau khi có số liệu, có thể vẽ biểu đồ bằng Python:

```python
import matplotlib.pyplot as plt

sizes = ['500MB', '1GB', '10GB']
mapreduce_times = [/* điền số liệu */]
spark_times = [/* điền số liệu */]

plt.plot(sizes, mapreduce_times, marker='o', label='MapReduce')
plt.plot(sizes, spark_times, marker='s', label='Spark')
plt.xlabel('Dung lượng dữ liệu')
plt.ylabel('Thời gian chạy (giây)')
plt.title('So sánh tốc độ MapReduce vs Spark')
plt.legend()
plt.grid(True)
plt.savefig('comparison_chart.png', dpi=150)
print("Đã lưu biểu đồ: comparison_chart.png")
```
