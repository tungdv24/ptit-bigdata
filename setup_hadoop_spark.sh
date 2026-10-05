#!/bin/bash
###############################################################################
# Script tự động cài đặt & khởi động Hadoop + Spark Standalone (Pseudo-Distributed)
# Dựa theo Lab1-Hadoop-Standalone-Setup.md
#
# Cách dùng:
#   Chạy dưới tài khoản hdoop:
#     su - hdoop
#     bash setup_hadoop_spark.sh install     # Cài đặt lần đầu (tải + cấu hình + format)
#     bash setup_hadoop_spark.sh update       # Ghi lại cấu hình mới (KHÔNG tải/format lại)
#     bash setup_hadoop_spark.sh start        # Khởi động cụm
#     bash setup_hadoop_spark.sh stop         # Dừng cụm
#     bash setup_hadoop_spark.sh status       # Kiểm tra tiến trình (jps)
#     bash setup_hadoop_spark.sh restart      # Khởi động lại
#
# LƯU Ý: Script này giả định user 'hdoop' và Java đã cài sẵn (openjdk-11).
###############################################################################

set -e  # Dừng nếu có lệnh lỗi

# ============================ CẤU HÌNH PHIÊN BẢN ============================
HADOOP_VERSION="3.3.6"
SPARK_VERSION="3.5.9"

JAVA_HOME_DIR="/usr/lib/jvm/java-11-openjdk-amd64"
INSTALL_DIR="$HOME"
HADOOP_HOME="$INSTALL_DIR/hadoop-$HADOOP_VERSION"
SPARK_HOME="$INSTALL_DIR/spark-$SPARK_VERSION-bin-hadoop3"

HADOOP_URL="https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz"
SPARK_URL="https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz"

DATA_DIR="/app/hadoop"

# ============================ CẤU HÌNH TÀI NGUYÊN ============================
# Máy: 8 core / 16GB RAM
# YARN quản lý 12GB / 8 vCores (chừa 4GB + tải cho OS)
# Mỗi job (MapReduce & Spark) dùng CÙNG mức: 8GB RAM / 4 vCores để so sánh công bằng.

# --- YARN (nền chung) ---
YARN_TOTAL_MEM=12288          # Tổng RAM YARN quản lý (MB) = 12GB
YARN_MAX_CONTAINER_MEM=8192   # RAM tối đa 1 container (MB) = 8GB
YARN_MIN_CONTAINER_MEM=512
YARN_TOTAL_VCORES=8           # Tổng vCores YARN quản lý
YARN_MAX_CONTAINER_VCORES=4   # vCores tối đa 1 container

# --- MapReduce (tổng ~8GB / 4 vCores: AM 2GB + Map 3GB + Reduce 3GB) ---
MR_AM_MEM=2048
MR_AM_VCORES=1
MR_MAP_MEM=3072
MR_MAP_VCORES=2
MR_REDUCE_MEM=3072
MR_REDUCE_VCORES=2

# --- Spark (tổng ~8GB / 4 vCores: Driver 2GB + Executor 5GB + overhead 1GB) ---
SPARK_DRIVER_MEM="2g"
SPARK_EXECUTOR_MEM="5g"
SPARK_EXECUTOR_OVERHEAD=1024
SPARK_EXECUTOR_CORES=4
SPARK_EXECUTOR_INSTANCES=1

# Màu cho output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; }

# Export biến môi trường cho phiên hiện tại
export JAVA_HOME="$JAVA_HOME_DIR"
export HADOOP_HOME="$HADOOP_HOME"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="$SPARK_HOME"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin"
export PYSPARK_PYTHON=python3

# ============================ HÀM CÀI ĐẶT ============================

check_java() {
    log "Kiểm tra Java..."
    if [ ! -d "$JAVA_HOME_DIR" ]; then
        err "Không tìm thấy Java tại $JAVA_HOME_DIR"
        err "Cài đặt bằng: sudo apt install openjdk-11-jdk -y"
        exit 1
    fi
    java -version 2>&1 | head -1
}

download_extract() {
    local url="$1"
    local target_dir="$2"
    local name="$3"
    local tarball="$INSTALL_DIR/$(basename "$url")"

    if [ -d "$target_dir" ]; then
        warn "$name đã tồn tại tại $target_dir — bỏ qua tải."
        return
    fi

    log "Tải $name..."
    if [ ! -f "$tarball" ]; then
        wget -q --show-progress "$url" -O "$tarball"
    fi

    log "Giải nén $name..."
    tar -xzf "$tarball" -C "$INSTALL_DIR"
    log "$name đã giải nén vào $target_dir"
}

setup_bashrc() {
    log "Thiết lập biến môi trường trong ~/.bashrc..."

    # Chỉ thêm nếu chưa có
    if grep -q "HADOOP_HOME=$HADOOP_HOME" "$HOME/.bashrc" 2>/dev/null; then
        warn "Biến môi trường Hadoop đã có trong .bashrc — bỏ qua."
        return
    fi

    cat >> "$HOME/.bashrc" << EOF

# ===== Hadoop & Spark Environment (auto-generated) =====
export JAVA_HOME=$JAVA_HOME_DIR
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_INSTALL=\$HADOOP_HOME
export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export HADOOP_YARN_HOME=\$HADOOP_HOME
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=\$HADOOP_HOME/lib/native"
export SPARK_HOME=$SPARK_HOME
export PYSPARK_PYTHON=python3
export PATH=\$PATH:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin
EOF
    log "Đã thêm biến môi trường vào .bashrc"
}

setup_data_dirs() {
    log "Tạo thư mục dữ liệu HDFS tại $DATA_DIR..."
    sudo mkdir -p "$DATA_DIR/tmp"
    sudo mkdir -p "$DATA_DIR/hdfs/namenode"
    sudo mkdir -p "$DATA_DIR/hdfs/datanode"
    sudo chown -R "$USER:$USER" "$DATA_DIR"
    sudo chmod -R 750 "$DATA_DIR"
}

configure_hadoop() {
    log "Cấu hình các file XML của Hadoop..."
    local conf="$HADOOP_HOME/etc/hadoop"

    # hadoop-env.sh — set JAVA_HOME
    if ! grep -q "^export JAVA_HOME=$JAVA_HOME_DIR" "$conf/hadoop-env.sh"; then
        echo "export JAVA_HOME=$JAVA_HOME_DIR" >> "$conf/hadoop-env.sh"
    fi

    # core-site.xml
    cat > "$conf/core-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>$DATA_DIR/tmp</value>
    </property>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

    # hdfs-site.xml
    cat > "$conf/hdfs-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://$DATA_DIR/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://$DATA_DIR/hdfs/datanode</value>
    </property>
</configuration>
EOF

    # mapred-site.xml (dùng biến tài nguyên ở đầu file)
    cat > "$conf/mapred-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.resource.mb</name>
        <value>$MR_AM_MEM</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.resource.cpu-vcores</name>
        <value>$MR_AM_VCORES</value>
    </property>
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>$MR_MAP_MEM</value>
    </property>
    <property>
        <name>mapreduce.map.cpu.vcores</name>
        <value>$MR_MAP_VCORES</value>
    </property>
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>$MR_REDUCE_MEM</value>
    </property>
    <property>
        <name>mapreduce.reduce.cpu.vcores</name>
        <value>$MR_REDUCE_VCORES</value>
    </property>
</configuration>
EOF

    # yarn-site.xml (dùng biến tài nguyên ở đầu file)
    cat > "$conf/yarn-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>$YARN_TOTAL_MEM</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>$YARN_MAX_CONTAINER_MEM</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>$YARN_MIN_CONTAINER_MEM</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>$YARN_TOTAL_VCORES</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>$YARN_MAX_CONTAINER_VCORES</value>
    </property>
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>
EOF

    log "Đã ghi core-site, hdfs-site, mapred-site, yarn-site."
}

configure_spark() {
    log "Cấu hình Spark..."
    local conf="$SPARK_HOME/conf"

    # spark-env.sh
    cat > "$conf/spark-env.sh" << EOF
#!/usr/bin/env bash
export JAVA_HOME=$JAVA_HOME_DIR
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=\$($HADOOP_HOME/bin/hadoop classpath)
export PYSPARK_PYTHON=python3
EOF
    chmod +x "$conf/spark-env.sh"

    # spark-defaults.conf (dùng biến tài nguyên ở đầu file)
    cat > "$conf/spark-defaults.conf" << EOF
spark.master                     yarn
spark.submit.deployMode          client
spark.driver.memory              $SPARK_DRIVER_MEM
spark.executor.memory            $SPARK_EXECUTOR_MEM
spark.executor.memoryOverhead    $SPARK_EXECUTOR_OVERHEAD
spark.executor.cores             $SPARK_EXECUTOR_CORES
spark.executor.instances         $SPARK_EXECUTOR_INSTANCES
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://localhost:9000/spark-logs
spark.history.fs.logDirectory    hdfs://localhost:9000/spark-logs
EOF
    log "Đã ghi spark-env.sh và spark-defaults.conf."
}

format_hdfs() {
    if [ -d "$DATA_DIR/hdfs/namenode/current" ]; then
        warn "NameNode đã được format trước đó — bỏ qua để tránh mất dữ liệu."
        warn "Nếu muốn format lại: xóa $DATA_DIR/hdfs/* rồi chạy lại."
        return
    fi
    log "Định dạng HDFS NameNode (lần đầu)..."
    "$HADOOP_HOME/bin/hdfs" namenode -format -force
}

create_spark_logs_dir() {
    log "Tạo thư mục /spark-logs trên HDFS..."
    "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /spark-logs 2>/dev/null || true
    "$HADOOP_HOME/bin/hdfs" dfs -chmod 777 /spark-logs 2>/dev/null || true
    "$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /user/$USER 2>/dev/null || true
}

# ============================ HÀM ĐIỀU KHIỂN ============================

do_install() {
    log "===== BẮT ĐẦU CÀI ĐẶT HADOOP + SPARK ====="
    check_java
    download_extract "$HADOOP_URL" "$HADOOP_HOME" "Hadoop $HADOOP_VERSION"
    download_extract "$SPARK_URL" "$SPARK_HOME" "Spark $SPARK_VERSION"
    setup_bashrc
    setup_data_dirs
    configure_hadoop
    configure_spark
    format_hdfs
    log "===== CÀI ĐẶT HOÀN TẤT ====="
    log "Chạy: bash $0 start   để khởi động cụm"
}

do_update() {
    log "===== CẬP NHẬT CẤU HÌNH (KHÔNG tải/format lại) ====="

    # Kiểm tra Hadoop/Spark đã cài chưa
    if [ ! -d "$HADOOP_HOME" ] || [ ! -d "$SPARK_HOME" ]; then
        err "Chưa cài Hadoop/Spark. Hãy chạy: bash $0 install"
        exit 1
    fi

    # Cảnh báo nếu cụm đang chạy — cần restart để nạp config mới
    local running=""
    if jps 2>/dev/null | grep -qE "NameNode|ResourceManager"; then
        running="yes"
        warn "Cụm đang chạy. Cấu hình mới chỉ có hiệu lực sau khi khởi động lại."
    fi

    configure_hadoop
    configure_spark

    log "===== ĐÃ CẬP NHẬT CẤU HÌNH ====="
    if [ "$running" = "yes" ]; then
        warn "Chạy để áp dụng:  bash $0 restart"
    else
        log "Chạy để khởi động: bash $0 start"
    fi
}

do_start() {
    log "===== KHỞI ĐỘNG HADOOP + SPARK ====="
    "$HADOOP_HOME/sbin/start-dfs.sh"
    "$HADOOP_HOME/sbin/start-yarn.sh"
    sleep 3
    create_spark_logs_dir
    "$SPARK_HOME/sbin/start-history-server.sh" || warn "Không khởi động được History Server (có thể HDFS chưa sẵn sàng)."
    echo ""
    do_status
    echo ""
    log "===== CÁC GIAO DIỆN WEB ====="
    echo "  HDFS NameNode:          http://localhost:9870"
    echo "  YARN ResourceManager:   http://localhost:8088"
    echo "  Spark History Server:   http://localhost:18080"
}

do_stop() {
    log "===== DỪNG HADOOP + SPARK ====="
    "$SPARK_HOME/sbin/stop-history-server.sh" 2>/dev/null || true
    "$HADOOP_HOME/sbin/stop-yarn.sh" 2>/dev/null || true
    "$HADOOP_HOME/sbin/stop-dfs.sh" 2>/dev/null || true
    log "Đã dừng tất cả dịch vụ."
}

do_status() {
    log "Các tiến trình Java đang chạy (jps):"
    jps | grep -v Jps || true
    echo ""
    # Kiểm tra đủ 5 tiến trình cốt lõi
    local procs=$(jps)
    for p in NameNode DataNode SecondaryNameNode ResourceManager NodeManager; do
        if echo "$procs" | grep -q "$p"; then
            echo -e "  ${GREEN}✓${NC} $p"
        else
            echo -e "  ${RED}✗${NC} $p (chưa chạy)"
        fi
    done
}

do_restart() {
    do_stop
    sleep 3
    do_start
}

# ============================ ĐIỀU HƯỚNG ============================

case "${1:-}" in
    install)  do_install ;;
    update)   do_update ;;
    start)    do_start ;;
    stop)     do_stop ;;
    status)   do_status ;;
    restart)  do_restart ;;
    *)
        echo "Cách dùng: bash $0 {install|update|start|stop|status|restart}"
        echo ""
        echo "  install  - Tải, cấu hình và format HDFS (chạy 1 lần đầu)"
        echo "  update   - Ghi lại cấu hình mới (KHÔNG tải/format lại). Dùng khi đổi RAM/CPU"
        echo "  start    - Khởi động HDFS + YARN + Spark History Server"
        echo "  stop     - Dừng tất cả dịch vụ"
        echo "  status   - Kiểm tra tiến trình đang chạy (jps)"
        echo "  restart  - Dừng rồi khởi động lại (áp dụng cấu hình mới)"
        exit 1
        ;;
esac
