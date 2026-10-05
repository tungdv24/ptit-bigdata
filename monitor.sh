#!/bin/bash
###############################################################################
# monitor.sh — Theo dõi CPU/RAM của MapReduce & Spark theo thời gian thực.
#
# Chạy SONG SONG trong lúc benchmark (mở 1 terminal riêng để chạy script này).
# Cứ mỗi INTERVAL giây, in ra 1 dòng có mốc thời gian gồm:
#   - Tài nguyên YARN cấp cho từng job đang chạy (RAM, vCores, containers)
#   - CPU/RAM tổng của server (để đối chiếu)
#
# Cách dùng:
#   bash monitor.sh              # theo dõi mỗi 3 giây (mặc định)
#   bash monitor.sh 5            # theo dõi mỗi 5 giây
#   bash monitor.sh 3 monitor.log  # đồng thời lưu ra file monitor.log
#
# Dừng: nhấn Ctrl+C
###############################################################################

INTERVAL="${1:-3}"                       # Khoảng thời gian giữa các lần đo (giây)
LOG_FILE="${2:-}"                        # File log (tùy chọn)
YARN_API="http://localhost:8088/ws/v1/cluster/apps?states=RUNNING"

# Màu
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Hàm ghi: vừa in màn hình, vừa ghi file nếu có
out() {
    if [ -n "$LOG_FILE" ]; then
        echo -e "$1" | tee -a "$LOG_FILE"
    else
        echo -e "$1"
    fi
}

# Header
out "=================================================================="
out " MONITOR CPU/RAM — MapReduce & Spark   (mỗi ${INTERVAL}s, Ctrl+C để dừng)"
out "=================================================================="

# Bắt Ctrl+C để thoát gọn
trap 'echo ""; echo "Đã dừng monitor."; exit 0' INT

while true; do
    TS=$(date '+%Y-%m-%d %H:%M:%S')

    # ---- Tài nguyên tổng của server (mức OS) ----
    # CPU: lấy %us + %sy từ top (1 lần)
    CPU_USED=$(top -bn1 | grep "Cpu(s)" | awk '{print $2 + $4}')
    # RAM: dùng free
    MEM_INFO=$(free -m | awk '/^Mem:/ {printf "%d/%d MB (%.0f%%)", $3, $2, $3/$2*100}')

    out ""
    out "${CYAN}[$TS]${NC}  Server → CPU: ${CPU_USED}%  |  RAM: ${MEM_INFO}"

    # ---- Tài nguyên YARN cấp cho từng job đang chạy ----
    APPS_JSON=$(curl -s --connect-timeout 5 "$YARN_API" 2>/dev/null)

    if [ -z "$APPS_JSON" ]; then
        out "  ${YELLOW}(Không kết nối được YARN API)${NC}"
    else
        # Parse JSON bằng python3
        PARSED=$(echo "$APPS_JSON" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
apps = (data.get('apps') or {}).get('app') or []
if not apps:
    print('  (Không có job nào đang chạy)')
else:
    for a in apps:
        name = a.get('name','?')[:30]
        atype = a.get('applicationType','?')
        mb = a.get('allocatedMB',0)
        vc = a.get('allocatedVCores',0)
        cont = a.get('runningContainers',0)
        prog = a.get('progress',0)
        print(f'  → [{atype:9}] {name:30} | RAM: {mb:6} MB | vCores: {vc} | Containers: {cont} | Tiến độ: {prog:.0f}%')
")
        out "$PARSED"
    fi

    sleep "$INTERVAL"
done
