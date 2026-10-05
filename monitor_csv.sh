#!/bin/bash
###############################################################################
# monitor_csv.sh — Theo dõi CPU/RAM theo thời gian, ghi ra file CSV để vẽ biểu đồ.
#
# Chạy SONG SONG trong lúc benchmark. Mỗi INTERVAL giây ghi 1 dòng CSV gồm:
#   timestamp, elapsed_sec, server_cpu, server_ram_used_mb, server_ram_pct,
#   mr_ram_mb, mr_vcores, spark_ram_mb, spark_vcores
#
# Cách dùng:
#   bash monitor_csv.sh                    # ghi vào monitor_data.csv, đo mỗi 2s
#   bash monitor_csv.sh 3 my_data.csv      # đo mỗi 3s, ghi vào my_data.csv
#
# Dừng: Ctrl+C. Sau đó vẽ biểu đồ: python3 plot_monitor.py monitor_data.csv
###############################################################################

INTERVAL="${1:-2}"
CSV_FILE="${2:-monitor_data.csv}"
YARN_API="http://localhost:8088/ws/v1/cluster/apps?states=RUNNING"

# Ghi header CSV (thêm cột epoch để khớp với file phases khi vẽ biểu đồ)
echo "epoch,timestamp,elapsed_sec,server_cpu,server_ram_mb,server_ram_pct,mr_ram_mb,mr_vcores,spark_ram_mb,spark_vcores" > "$CSV_FILE"

echo "Đang ghi số liệu vào: $CSV_FILE (mỗi ${INTERVAL}s). Nhấn Ctrl+C để dừng."
echo "Sau khi dừng, vẽ biểu đồ bằng: python3 plot_monitor.py $CSV_FILE [phases_file.csv]"

START_TS=$(date +%s)
trap 'echo ""; echo "Đã dừng. Dữ liệu lưu tại: '"$CSV_FILE"'"; exit 0' INT

while true; do
    NOW=$(date +%s)
    ELAPSED=$((NOW - START_TS))
    TS=$(date '+%H:%M:%S')

    # CPU tổng server (%us + %sy)
    CPU=$(top -bn1 | grep "Cpu(s)" | awk '{print $2 + $4}')
    # RAM tổng server
    read RAM_MB RAM_PCT <<< $(free -m | awk '/^Mem:/ {printf "%d %.0f", $3, $3/$2*100}')

    # Lấy tài nguyên YARN theo loại job
    RESOURCES=$(curl -s --connect-timeout 5 "$YARN_API" 2>/dev/null | python3 -c "
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
    [ -z "$RESOURCES" ] && RESOURCES="0,0,0,0"

    echo "$NOW,$TS,$ELAPSED,$CPU,$RAM_MB,$RAM_PCT,$RESOURCES" >> "$CSV_FILE"
    echo "[$TS] +${ELAPSED}s  CPU:${CPU}%  RAM:${RAM_MB}MB(${RAM_PCT}%)  YARN(MR,SP):$RESOURCES"

    sleep "$INTERVAL"
done
