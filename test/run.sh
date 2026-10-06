#!/usr/bin/env bash
###############################################################################
# run.sh — Benchmark so sánh hiệu năng Hadoop MapReduce vs Apache Spark
#           trên một tệp dữ liệu HDFS tùy chọn.
#
# Cách dùng:
#   bash run.sh <ten_file_hoac_duong_dan_hdfs>
# Ví dụ:
#   bash run.sh numbers_1gb.txt
#   bash run.sh /user/hdoop/prime/input/numbers_500mb.txt
#
# Tự động xuất đầy đủ:
#   - Thư mục kết quả riêng: results/run_<timestamp>_<ten_file>/
#   - resource_chart.png : Dashboard 4 ô gộp t=0 (CPU, RAM, YARN vCores, YARN RAM)
#   - timeline_chart.png : Biểu đồ toàn cảnh theo dòng thời gian thực
#   - disk_io_chart.png  : Biểu đồ giám sát Disk IOPS & Throughput MB/s
#   - summary.txt        : Bảng đối chiếu kết quả, runtime, speedup và số nguyên tố
###############################################################################

set -u

# Luôn chuyển đến thư mục chứa script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ======================= XỬ LÝ ĐỐI SỐ ĐẦU VÀO =======================
INPUT_ARG="${1:-}"

if [ -z "$INPUT_ARG" ]; then
    echo "=================================================================="
    echo " [!] THIẾU THAM SỐ ĐẦU VÀO"
    echo " Cách dùng: bash run.sh <ten_file_hoac_duong_dan_hdfs>"
    echo " Ví dụ:     bash run.sh numbers_1gb.txt"
    echo "            bash run.sh numbers_500mb.txt"
    echo "            bash run.sh /user/hdoop/prime/input/numbers_10gb.txt"
    echo "=================================================================="
    exit 1
fi

# Nếu người dùng truyền đường dẫn tuyệt đối bắt đầu bằng / thì giữ nguyên
# Ngược lại, tự động thêm prefix /user/hdoop/prime/input/
if [[ "$INPUT_ARG" == /* ]]; then
    HDFS_INPUT="$INPUT_ARG"
    FILE_NAME=$(basename "$INPUT_ARG")
else
    HDFS_INPUT="/user/hdoop/prime/input/$INPUT_ARG"
    FILE_NAME="$INPUT_ARG"
fi

# Tên nhãn (bỏ phần mở rộng .txt)
LABEL="${FILE_NAME%.*}"

# ======================= THIẾT LẬP MÔI TRƯỜNG =======================
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/home/hdoop/hadoop-3.3.6}"
export SPARK_HOME="${SPARK_HOME:-/home/hdoop/spark-3.5.9-bin-hadoop3}"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin"

# Tìm python có matplotlib để vẽ biểu đồ
if [ -x "/home/hdoop/monitor/venv/bin/python3" ]; then
    PY_PLOT="/home/hdoop/monitor/venv/bin/python3"
elif [ -x "./venv/bin/python3" ]; then
    PY_PLOT="./venv/bin/python3"
elif [ -x "../venv/bin/python3" ]; then
    PY_PLOT="../venv/bin/python3"
else
    PY_PLOT="python3"
fi

STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
YARN_API="http://localhost:8088/ws/v1/cluster/apps?states=RUNNING"

# Thư mục lưu kết quả riêng cho lần chạy này
RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUT_DIR="results/run_${RUN_TIMESTAMP}_${LABEL}"
mkdir -p "$OUT_DIR"

LOG_SUMMARY="$OUT_DIR/summary.txt"
PHASES_CSV="$OUT_DIR/phases.csv"
MONITOR_CSV="$OUT_DIR/monitor_data.csv"
CHART_RESOURCE="$OUT_DIR/resource_chart.png"
CHART_TIMELINE="$OUT_DIR/timeline_chart.png"
CHART_DISK="$OUT_DIR/disk_io_chart.png"
MR_LOG="$OUT_DIR/mr_progress.log"
SPARK_LOG="$OUT_DIR/spark_progress.log"

echo "phase,start_epoch,end_epoch" > "$PHASES_CSV"

# ======================= KIỂM TRA FILE TRÊN HDFS =======================
echo "================================================================================"
echo "    BENCHMARK SO SÁNH HIỆU NĂNG: HADOOP MAPREDUCE VS APACHE SPARK"
echo "    Mục tiêu: Đếm số lượng số nguyên tố (Prime Counting)"
echo "    Chế độ tài nguyên: Cân bằng 1:1 (Tối đa 8GB RAM / 4 compute vCores)"
echo "================================================================================"
echo ">>> Kiểm tra tệp HDFS: $HDFS_INPUT ..."

if ! hdfs dfs -test -e "$HDFS_INPUT" 2>/dev/null; then
    echo ""
    echo "[LỖI NGHIÊM TRỌNG] Tệp không tồn tại trên HDFS: $HDFS_INPUT"
    echo "Các tệp hiện có trong /user/hdoop/prime/input/:"
    hdfs dfs -ls -h /user/hdoop/prime/input/ 2>/dev/null || echo " (Thư mục trống hoặc không tồn tại)"
    exit 1
fi

FILE_SIZE_INFO=$(hdfs dfs -ls -h "$HDFS_INPUT" 2>/dev/null | awk '{print $5}')
echo "✓ Tìm thấy tệp trên HDFS (Dung lượng: $FILE_SIZE_INFO)"
echo ">>> Thư mục lưu kết quả: $OUT_DIR"
echo ">>> Bắt đầu lúc: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

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

trap 'echo ""; echo "[!] Nhận tín hiệu kết thúc/ngắt..."; stop_monitor; exit' INT TERM EXIT

# ======================= HÀM MONITOR NỘI TẠI =======================
start_monitor() {
    local csv_file="$1"
    local interval="${2:-2}"

    echo "epoch,timestamp,elapsed_sec,server_cpu,server_ram_mb,server_ram_pct,mr_ram_mb,mr_vcores,spark_ram_mb,spark_vcores,disk_read_iops,disk_write_iops,disk_read_mb_s,disk_write_mb_s" > "$csv_file"

    (
        start_ts=$(date +%s)

        # Đọc CPU ban đầu qua /proc/stat
        read -r prev_idle prev_total <<< $(awk '/^cpu / {print $5+$6, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat 2>/dev/null || echo "0 0")
        # Đọc Disk stats ban đầu cho sda qua /proc/diskstats
        read -r prev_r prev_w prev_sr prev_sw <<< $(awk '$3 == "sda" {print $4, $8, $6, $10}' /proc/diskstats 2>/dev/null || echo "0 0 0 0")

        while true; do
            now_ts=$(date +%s)
            elapsed=$((now_ts - start_ts))
            ts_str=$(date '+%H:%M:%S')

            sleep "$interval"

            # Đọc delta CPU qua /proc/stat
            read -r idle2 total2 <<< $(awk '/^cpu / {print $5+$6, $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat 2>/dev/null || echo "0 0")
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
            read -r r2 w2 sr2 sw2 <<< $(awk '$3 == "sda" {print $4, $8, $6, $10}' /proc/diskstats 2>/dev/null || echo "0 0 0 0")
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
            read -r ram_mb ram_pct <<< $(free -m 2>/dev/null | awk '/^Mem:/ {printf "%d %.0f", $3, $3/$2*100}')

            # Truy vấn YARN REST API
            resources=$(curl -s --connect-timeout 2 "$YARN_API" 2>/dev/null | python3 -c "
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
    echo ">>> Monitor tài nguyên đã khởi động (PID: $MONITOR_PID, chu kỳ: ${interval}s) -> $csv_file"
}

# ======================= BẮT ĐẦU QUÁ TRÌNH BENCHMARK =======================
start_monitor "$MONITOR_CSV" 2
sleep 3  # Lấy 1-2 mẫu baseline lúc hệ thống rảnh

# -----------------------------------------------------------------------------
# 1. CHẠY HADOOP MAPREDUCE
# -----------------------------------------------------------------------------
echo ""
echo "--------------------------------------------------------------------------------"
echo ">>> [1/2] Đang chạy HADOOP MAPREDUCE..."
echo "--------------------------------------------------------------------------------"
hdfs dfs -rm -r /user/hdoop/prime/output_mr 2>/dev/null || true

MR_START_EPOCH=$(date +%s)
MR_START_HUMAN=$(date '+%H:%M:%S')

hadoop jar "$STREAMING_JAR" \
  -input "$HDFS_INPUT" \
  -output /user/hdoop/prime/output_mr \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py \
  -file reducer.py 2>&1 | tee "$MR_LOG"

MR_END_EPOCH=$(date +%s)
MR_END_HUMAN=$(date '+%H:%M:%S')
MR_RUNTIME=$((MR_END_EPOCH - MR_START_EPOCH))

echo "MapReduce,$MR_START_EPOCH,$MR_END_EPOCH" >> "$PHASES_CSV"

# Lấy output MapReduce
MR_FORMATTED=$(hdfs dfs -cat /user/hdoop/prime/output_mr/part-* 2>/dev/null | python3 format_mr_output.py 2>/dev/null || echo "Không đọc được output")
MR_PRIMES=$(echo "$MR_FORMATTED" | grep "Số nguyên tố" | grep -oE '[0-9]+' | head -n 1)

echo ""
echo ">>> MapReduce hoàn tất trong: $MR_RUNTIME giây ($MR_START_HUMAN -> $MR_END_HUMAN)"

# COOL-DOWN 15 GIÂY (Cho RAM/CPU trở về trạng thái nhàn rỗi, tạo phân cách đẹp trên biểu đồ)
echo ">>> Nghỉ giải lao 15 giây để hạ tải hệ thống trước khi chạy Spark..."
sleep 15

# -----------------------------------------------------------------------------
# 2. CHẠY APACHE SPARK
# -----------------------------------------------------------------------------
echo ""
echo "--------------------------------------------------------------------------------"
echo ">>> [2/2] Đang chạy APACHE SPARK..."
echo "--------------------------------------------------------------------------------"
SPARK_START_EPOCH=$(date +%s)
SPARK_START_HUMAN=$(date '+%H:%M:%S')

spark-submit --master yarn --deploy-mode client \
  prime_spark.py "$HDFS_INPUT" 2>&1 | tee "$SPARK_LOG"

SPARK_END_EPOCH=$(date +%s)
SPARK_END_HUMAN=$(date '+%H:%M:%S')
SPARK_RUNTIME=$((SPARK_END_EPOCH - SPARK_START_EPOCH))

echo "Spark,$SPARK_START_EPOCH,$SPARK_END_EPOCH" >> "$PHASES_CSV"

SPARK_FORMATTED=$(grep -E "Tổng số|Số nguyên tố|Số không nguyên tố" "$SPARK_LOG" || echo "Không tìm thấy kết quả trong log")
SPARK_PRIMES=$(echo "$SPARK_FORMATTED" | grep "Số nguyên tố" | grep -oE '[0-9]+' | head -n 1)

echo ""
echo ">>> Spark hoàn tất trong: $SPARK_RUNTIME giây ($SPARK_START_HUMAN -> $SPARK_END_HUMAN)"

# COOL-DOWN 5 GIÂY TRƯỚC KHI TẮT MONITOR
sleep 5
stop_monitor

# ======================= ĐỐI CHIẾU & TÍNH TOÁN =======================
SPEEDUP="0.00"
if [ "$SPARK_RUNTIME" -gt 0 ]; then
    SPEEDUP=$(awk -v mr="$MR_RUNTIME" -v sp="$SPARK_RUNTIME" 'BEGIN { printf "%.2f", mr / sp }')
fi

MATCH_STATUS="FAILED"
if [ -n "$MR_PRIMES" ] && [ "$MR_PRIMES" = "$SPARK_PRIMES" ]; then
    MATCH_STATUS="MATCHED"
fi

# Ghi file summary
{
    echo "================================================================================"
    echo "                      KẾT QUẢ BENCHMARK: $FILE_NAME"
    echo "================================================================================"
    echo "HDFS Input:             $HDFS_INPUT ($FILE_SIZE_INFO)"
    echo "Thời gian MapReduce:    $MR_RUNTIME giây ($MR_START_HUMAN -> $MR_END_HUMAN)"
    echo "Thời gian Spark:         $SPARK_RUNTIME giây ($SPARK_START_HUMAN -> $SPARK_END_HUMAN)"
    echo "Hệ số tăng tốc (Speedup): ${SPEEDUP}x (Spark nhanh hơn)"
    echo ""
    echo "--- KẾT QUẢ MAPREDUCE ---"
    echo "$MR_FORMATTED"
    echo ""
    echo "--- KẾT QUẢ SPARK ---"
    echo "$SPARK_FORMATTED"
    echo ""
    echo "--- ĐỐI CHIẾU KẾT QUẢ SỐ NGUYÊN TỐ ---"
    if [ "$MATCH_STATUS" = "MATCHED" ]; then
        echo "✓ KẾT QUẢ KHỚP TUYỆT ĐỐI 100%: Cả 2 đều đếm được $MR_PRIMES số nguyên tố."
    else
        echo "✗ CẢNH BÁO LỆCH KẾT QUẢ: MapReduce=$MR_PRIMES | Spark=$SPARK_PRIMES"
    fi
    echo "================================================================================"
} | tee "$LOG_SUMMARY"

# ======================= VẼ TOÀN BỘ DASHBOARD BIỂU ĐỒ =======================
if [ -f "plot_monitor.py" ]; then
    echo ""
    echo ">>> Đang sinh bộ biểu đồ Dashboard tài nguyên và Disk I/O..."
    $PY_PLOT plot_monitor.py "$MONITOR_CSV" "$PHASES_CSV" "$CHART_RESOURCE" 2>&1 || true
fi

echo ""
echo "================================================================================"
echo "    BENCHMARK HOÀN TẤT THÀNH CÔNG!"
echo "================================================================================"
echo " Thư mục kết quả: $OUT_DIR"
echo " 1. Dashboard So Sánh Gộp (t=0) : $CHART_RESOURCE"
echo " 2. Biểu đồ Giám Sát Disk I/O   : $CHART_DISK"
echo " 3. Biểu đồ Dòng Thời Gian Thực : $CHART_TIMELINE"
echo " 4. Báo cáo Tóm Tắt             : $LOG_SUMMARY"
echo " 5. Dữ liệu Thô (CSV)           : $MONITOR_CSV"
echo "================================================================================"
