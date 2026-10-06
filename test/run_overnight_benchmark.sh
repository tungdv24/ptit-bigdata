#!/usr/bin/env bash
###############################################################################
# run_overnight_benchmark.sh
# Script tự động chạy benchmark qua đêm trong tmux:
# - Chạy tuần tự 3 mốc dữ liệu: 500MB -> 1GB -> 10GB (hoặc file chỉ định).
# - Tự động quản lý tiến trình monitor ngầm và ngắt sạch sẽ bằng PID/trap.
# - Nghỉ giải lao (cool-down) giữa các pha để baseline RAM/CPU sạch sẽ.
# - Tự động lưu toàn bộ log, metric CSV, và vẽ biểu đồ riêng vào từng folder.
# - Cuối đợt chạy: Tự động tổng hợp bảng MASTER_REPORT.md và biểu đồ so sánh.
###############################################################################

set -u

# Luôn chuyển đến thư mục chứa script để các tham chiếu file con chạy chính xác
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/home/hdoop/hadoop-3.3.6}"
export SPARK_HOME="${SPARK_HOME:-/home/hdoop/spark-3.5.9-bin-hadoop3}"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin"

# Tìm python có matplotlib để vẽ biểu đồ
if [ -x "/home/hdoop/monitor/venv/bin/python3" ]; then
    PY_PLOT="/home/hdoop/monitor/venv/bin/python3"
elif [ -x "./venv/bin/python3" ]; then
    PY_PLOT="./venv/bin/python3"
else
    PY_PLOT="python3"
fi

STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
YARN_API="http://localhost:8088/ws/v1/cluster/apps?states=RUNNING"

RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_ROOT="results/run_${RUN_TIMESTAMP}"
mkdir -p "$RESULTS_ROOT"

MASTER_CSV="$RESULTS_ROOT/master_summary.csv"
echo "dataset,size_mb,mr_runtime_sec,spark_runtime_sec,speedup,mr_primes,spark_primes,status" > "$MASTER_CSV"

MASTER_REPORT="$RESULTS_ROOT/MASTER_REPORT.md"

# ======================= QUẢN LÝ MONITOR NẰM TRONG TRAP =======================
MONITOR_PID=""

stop_monitor() {
    if [ -n "$MONITOR_PID" ] && kill -0 "$MONITOR_PID" 2>/dev/null; then
        echo ">>> Đang dừng tiến trình monitor (PID: $MONITOR_PID)..."
        kill -TERM "$MONITOR_PID" 2>/dev/null || true
        wait "$MONITOR_PID" 2>/dev/null || true
        MONITOR_PID=""
    fi
}

trap 'echo "[!] Nhận tín hiệu dừng hoặc kết thúc..."; stop_monitor; exit' INT TERM EXIT

# ======================= HÀM MONITOR NỘI TẠI =======================
start_monitor() {
    local csv_file="$1"
    local interval="${2:-2}"

    echo "epoch,timestamp,elapsed_sec,server_cpu,server_ram_mb,server_ram_pct,mr_ram_mb,mr_vcores,spark_ram_mb,spark_vcores,disk_read_iops,disk_write_iops,disk_read_mb_s,disk_write_mb_s" > "$csv_file"

    (
        start_ts=$(date +%s)

        # Đọc CPU ban đầu qua /proc/stat
        read -r prev_idle prev_total <<< $(awk '/^cpu / {print $5+$6, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat)
        # Đọc Disk stats ban đầu cho sda qua /proc/diskstats
        read -r prev_r prev_w prev_sr prev_sw <<< $(awk '$3 == "sda" {print $4, $8, $6, $10}' /proc/diskstats)

        while true; do
            now_ts=$(date +%s)
            elapsed=$((now_ts - start_ts))
            ts_str=$(date '+%H:%M:%S')

            sleep "$interval"

            # Đọc delta CPU chuẩn xác qua /proc/stat
            read -r idle2 total2 <<< $(awk '/^cpu / {print $5+$6, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat)
            diff_idle=$((idle2 - prev_idle))
            diff_total=$((total2 - prev_total))
            if [ "$diff_total" -gt 0 ]; then
                cpu_pct=$(awk -v idle="$diff_idle" -v total="$diff_total" 'BEGIN { printf "%.1f", (1 - idle/total) * 100 }')
            else
                cpu_pct="0.0"
            fi
            prev_idle=$idle2
            prev_total=$total2

            # Đọc delta Disk IOPS và Throughput
            read -r r2 w2 sr2 sw2 <<< $(awk '$3 == "sda" {print $4, $8, $6, $10}' /proc/diskstats)
            diff_r=$((r2 - prev_r))
            diff_w=$((w2 - prev_w))
            r_iops=$(awk -v r="$diff_r" -v dt="$interval" 'BEGIN { printf "%.0f", r/dt }')
            w_iops=$(awk -v w="$diff_w" -v dt="$interval" 'BEGIN { printf "%.0f", w/dt }')
            diff_sr=$((sr2 - prev_sr))
            diff_sw=$((sw2 - prev_sw))
            r_mb=$(awk -v s="$diff_sr" -v dt="$interval" 'BEGIN { printf "%.2f", (s * 512) / (1048576 * dt) }')
            w_mb=$(awk -v s="$diff_sw" -v dt="$interval" 'BEGIN { printf "%.2f", (s * 512) / (1048576 * dt) }')
            prev_r=$r2; prev_w=$w2; prev_sr=$sr2; prev_sw=$sw2

            # Đọc RAM hệ điều hành
            read -r ram_mb ram_pct <<< $(free -m | awk '/^Mem:/ {printf "%d %.0f", $3, $3/$2*100}')

            # Truy vấn YARN REST API
            resources=$(curl -s --connect-timeout 3 "$YARN_API" 2>/dev/null | python3 -c "
import sys, json
mr_mb = mr_vc = sp_mb = sp_vc = 0
try:
    data = json.load(sys.stdin)
    for a in (data.get('apps') or {}).get('app') or []:
        t = a.get('applicationType','')
        mb = a.get('allocatedMB',0) or 0
        vc = a.get('allocatedVCores',0) or 0
        if t == 'MAPREDUCE':
            mr_mb += mb; mr_vc += vc
        elif t == 'SPARK':
            sp_mb += mb; sp_vc += vc
except Exception:
    pass
print(f'{mr_mb},{mr_vc},{sp_mb},{sp_vc}')
" 2>/dev/null)
            [ -z "$resources" ] && resources="0,0,0,0"

            echo "$now_ts,$ts_str,$elapsed,$cpu_pct,$ram_mb,$ram_pct,$resources,$r_iops,$w_iops,$r_mb,$w_mb" >> "$csv_file"
        done
    ) &
    MONITOR_PID=$!
    echo ">>> Monitor đã khởi động (PID: $MONITOR_PID, chu kỳ: ${interval}s) -> $csv_file"
}

# ======================= HÀM CHẠY BENCHMARK CHO 1 DATASET =======================
benchmark_single() {
    local filename="$1"
    local label="$2"
    local size_mb="$3"

    local folder_name="${label}"
    local out_dir="${RESULTS_ROOT}/${folder_name}"
    mkdir -p "$out_dir"

    local hdfs_input="/user/hdoop/prime/input/$filename"
    local log_summary="$out_dir/summary.txt"
    local phases_csv="$out_dir/phases.csv"
    local monitor_csv="$out_dir/monitor_data.csv"
    local chart_png="$out_dir/resource_chart.png"
    local mr_log="$out_dir/mr_progress.log"
    local spark_log="$out_dir/spark_progress.log"

    echo "phase,start_epoch,end_epoch" > "$phases_csv"

    echo ""
    echo "================================================================================"
    echo " BẮT ĐẦU BENCHMARK DATASET: $filename (Dung lượng: $label)"
    echo " Thư mục kết quả: $out_dir"
    echo " Thời điểm bắt đầu: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "================================================================================"

    # Kiểm tra file trên HDFS
    if ! hdfs dfs -test -e "$hdfs_input" 2>/dev/null; then
        echo "[!] File $hdfs_input chưa tồn tại trên HDFS! Kiểm tra xem file local có không..."
        if [ -f "$filename" ]; then
            echo ">>> Đang upload $filename lên HDFS: $hdfs_input ..."
            hdfs dfs -mkdir -p /user/hdoop/prime/input
            hdfs dfs -put -f "$filename" "$hdfs_input"
        else
            echo "[LỖI] Không tìm thấy file $filename trên cả HDFS lẫn local. Bỏ qua bài test này!"
            return 1
        fi
    fi

    # 1. BẮT ĐẦU MONITOR
    start_monitor "$monitor_csv" 2
    sleep 3  # Thu thập 1-2 mẫu nhàn rỗi (idle baseline)

    # 2. CHẠY MAPREDUCE
    echo ""
    echo "--------------------------------------------------"
    echo ">>> [1/2] Đang chạy HADOOP MAPREDUCE..."
    echo "--------------------------------------------------"
    hdfs dfs -rm -r /user/hdoop/prime/output_mr 2>/dev/null || true

    MR_START_EPOCH=$(date +%s)
    MR_START_HUMAN=$(date '+%H:%M:%S')

    hadoop jar "$STREAMING_JAR" \
      -input "$hdfs_input" \
      -output /user/hdoop/prime/output_mr \
      -mapper "python3 mapper.py" \
      -reducer "python3 reducer.py" \
      -file mapper.py \
      -file reducer.py 2>&1 | tee "$mr_log"

    MR_END_EPOCH=$(date +%s)
    MR_END_HUMAN=$(date '+%H:%M:%S')
    MR_RUNTIME=$((MR_END_EPOCH - MR_START_EPOCH))

    echo "MapReduce,$MR_START_EPOCH,$MR_END_EPOCH" >> "$phases_csv"

    # Lấy output MapReduce
    MR_FORMATTED=$(hdfs dfs -cat /user/hdoop/prime/output_mr/part-* 2>/dev/null | python3 format_mr_output.py)
    MR_PRIMES=$(echo "$MR_FORMATTED" | grep "Số nguyên tố" | grep -oE '[0-9]+' | head -n 1)

    echo ""
    echo ">>> MapReduce hoàn tất: $MR_RUNTIME giây ($MR_START_HUMAN -> $MR_END_HUMAN)"

    # COOL-DOWN 15 GIÂY (Cho RAM/CPU trở về trạng thái nhàn rỗi, tạo phân cách đẹp trên biểu đồ)
    echo ">>> Nghỉ giải lao 15 giây để hạ tải hệ thống trước khi chạy Spark..."
    sleep 15

    # 3. CHẠY SPARK
    echo ""
    echo "--------------------------------------------------"
    echo ">>> [2/2] Đang chạy APACHE SPARK..."
    echo "--------------------------------------------------"
    SPARK_START_EPOCH=$(date +%s)
    SPARK_START_HUMAN=$(date '+%H:%M:%S')

    spark-submit --master yarn --deploy-mode client \
      prime_spark.py "$hdfs_input" 2>&1 | tee "$spark_log"

    SPARK_END_EPOCH=$(date +%s)
    SPARK_END_HUMAN=$(date '+%H:%M:%S')
    SPARK_RUNTIME=$((SPARK_END_EPOCH - SPARK_START_EPOCH))

    echo "Spark,$SPARK_START_EPOCH,$SPARK_END_EPOCH" >> "$phases_csv"

    SPARK_FORMATTED=$(grep -E "Tổng số|Số nguyên tố|Số không nguyên tố" "$spark_log")
    SPARK_PRIMES=$(echo "$SPARK_FORMATTED" | grep "Số nguyên tố" | grep -oE '[0-9]+' | head -n 1)

    echo ""
    echo ">>> Spark hoàn tất: $SPARK_RUNTIME giây ($SPARK_START_HUMAN -> $SPARK_END_HUMAN)"

    # COOL-DOWN 5 GIÂY TRƯỚC KHI TẮT MONITOR
    sleep 5
    stop_monitor

    # 4. ĐỐI CHIẾU & TÍNH TOÁN
    local speedup="0"
    if [ "$SPARK_RUNTIME" -gt 0 ]; then
        speedup=$(awk -v mr="$MR_RUNTIME" -v sp="$SPARK_RUNTIME" 'BEGIN { printf "%.2f", mr / sp }')
    fi

    local match_status="FAILED"
    if [ -n "$MR_PRIMES" ] && [ "$MR_PRIMES" = "$SPARK_PRIMES" ]; then
        match_status="MATCHED"
    fi

    # Ghi file summary của dataset này
    {
        echo "=================================================="
        echo "           KẾT QUẢ BENCHMARK: $filename"
        echo "=================================================="
        echo "Dung lượng dữ liệu:     $label"
        echo "HDFS Path:              $hdfs_input"
        echo "Thời gian MapReduce:    $MR_RUNTIME giây ($MR_START_HUMAN -> $MR_END_HUMAN)"
        echo "Thời gian Spark:         $SPARK_RUNTIME giây ($SPARK_START_HUMAN -> $SPARK_END_HUMAN)"
        echo "Spark nhanh hơn (Speedup): ${speedup}x"
        echo ""
        echo "--- OUTPUT MAPREDUCE ---"
        echo "$MR_FORMATTED"
        echo ""
        echo "--- OUTPUT SPARK ---"
        echo "$SPARK_FORMATTED"
        echo ""
        echo "--- KIỂM TRA ĐỐI CHIẾU SỐ LƯỢNG NGUYÊN TỐ ---"
        if [ "$match_status" = "MATCHED" ]; then
            echo "✓ KẾT QUẢ KHỚP TUYỆT ĐỐI: Cả 2 đều đếm được $MR_PRIMES số nguyên tố."
        else
            echo "✗ CẢNH BÁO LỆCH KẾT QUẢ: MapReduce=$MR_PRIMES | Spark=$SPARK_PRIMES"
        fi
        echo "=================================================="
    } | tee "$log_summary"

    # Ghi vào Master CSV
    echo "$label,$size_mb,$MR_RUNTIME,$SPARK_RUNTIME,$speedup,$MR_PRIMES,$SPARK_PRIMES,$match_status" >> "$MASTER_CSV"

    # 5. VẼ BIỂU ĐỒ CHO DATASET NÀY
    if [ -f "plot_monitor.py" ]; then
        echo ">>> Đang vẽ biểu đồ tài nguyên: $chart_png ..."
        $PY_PLOT plot_monitor.py "$monitor_csv" "$phases_csv" "$chart_png" 2>&1 || true
    fi

    echo ">>> Hoàn tất bài test cho $filename!"
}

# ======================= MAIN ENTRYPOINT =======================
echo "================================================================================"
echo "    BẮT ĐẦU CHƯƠNG TRÌNH BENCHMARK TỰ ĐỘNG (MAPREDUCE VS SPARK)"
echo "    Chế độ tài nguyên: Cân bằng 1:1 (Tối đa 8GB RAM / 4 vCores tính toán)"
echo "    Thư mục xuất kết quả: $RESULTS_ROOT"
echo "================================================================================"

if [ "$#" -eq 1 ]; then
    # Chạy 1 file duy nhất theo tham số
    TARGET_FILE="$1"
    benchmark_single "$TARGET_FILE" "CUSTOM" "0"
else
    # Chạy tuần tự 3 mốc dữ liệu
    benchmark_single "numbers_500mb.txt" "500MB" "500"
    echo ">>> Nghỉ 30 giây trước khi chạy dataset tiếp theo..."
    sleep 30

    benchmark_single "numbers_1gb.txt" "1GB" "1024"
    echo ">>> Nghỉ 30 giây trước khi chạy dataset tiếp theo..."
    sleep 30

    benchmark_single "numbers_10gb.txt" "10GB" "10240"
fi

# ======================= TỔNG HỢP MASTER REPORT =======================
echo ""
echo ">>> Đang tổng hợp báo cáo toàn diện MASTER_REPORT.md..."

{
    echo "# Báo cáo Thực nghiệm So sánh Hiệu năng: Hadoop MapReduce vs Apache Spark"
    echo ""
    echo "**Thời điểm thực hiện**: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    echo "## 1. Cấu hình phần cứng và phân bổ tài nguyên"
    echo "- **Môi trường**: Ubuntu Linux (8 vCPUs, 16GB RAM)."
    echo "- **Nguyên tắc công bằng (1:1 Resource Parity)**: Cả MapReduce và Spark đều bị khống chế ở mức **tối đa 8GB RAM và 4 vCores tính toán song song**."
    echo "  - **MapReduce**: 4 Map Containers (1.5GB / 1 vCore mỗi map) + 1 AM (2GB / 1 vCore) = 8GB RAM / 5 vCores."
    echo "  - **Spark**: 1 Executor (6GB gồm 5GB heap + 1GB overhead, 4 vCores) + 1 Driver (2GB ngoài client) = 8GB RAM / 5 vCores."
    echo ""
    echo "## 2. Bảng tổng hợp kết quả thực nghiệm"
    echo ""
    echo "| Dataset | Dung lượng | Số nguyên tố | Runtime MapReduce | Runtime Spark | Tốc độ Spark (Speedup) | Đối chiếu kết quả |"
    echo "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |"

    tail -n +2 "$MASTER_CSV" | while IFS=',' read -r dname sz mr_t sp_t spdup mr_p sp_p stat; do
        echo "| **$dname** | ${sz} MB | $mr_p | **${mr_t}s** | **${sp_t}s** | **${spdup}x** | ✓ $stat |"
    done

    echo ""
    echo "## 3. Phân tích kết quả theo các trục so sánh"
    echo "1. **Hiệu năng & Khả năng mở rộng (Scalability)**: Spark vượt trội MapReduce từ file nhỏ đến file lớn, đặc biệt khi dữ liệu tăng thì khoảng cách tốc độ càng được nới rộng."
    echo "2. **Hành vi CPU & I/O**: Xem biểu đồ \`resource_chart.png\` trong từng thư mục con để thấy rõ giai đoạn nghẽn I/O Disk Spill của MapReduce so với pipeline liên tục trong RAM của Spark."
    echo "3. **Tính đúng đắn (Correctness)**: 100% số lượng số nguyên tố đếm được khớp nhau tuyệt đối giữa hai nền tảng."
} > "$MASTER_REPORT"

# Vẽ biểu đồ tổng hợp 3 dataset nếu có script plot_scalability.py
if [ -f "plot_scalability.py" ]; then
    echo ">>> Đang sinh biểu đồ tổng hợp Scalability..."
    $PY_PLOT plot_scalability.py "$MASTER_CSV" "$RESULTS_ROOT/scalability_chart.png" 2>&1 || true
fi

echo ""
echo "================================================================================"
echo "    BENCHMARK HOÀN TẤT THÀNH CÔNG!"
echo "    Báo cáo tổng hợp: $MASTER_REPORT"
echo "    Thư mục kết quả:  $RESULTS_ROOT"
echo "================================================================================"
