# Cấu hình đồng nhất: MapReduce & Spark cùng 4 core / 8GB RAM

> Mục tiêu: Cấp **cùng một lượng tài nguyên (4 vCores + 8GB RAM)** cho mỗi job của
> MapReduce và Spark, để so sánh tốc độ xử lý một cách công bằng.
>
> Cấu hình máy: **8 core / 16GB RAM**. YARN quản lý 12GB (chừa 4GB cho hệ điều hành),
> mỗi job dùng tối đa 8GB / 4 vCores.

---

## Tổng quan phân bổ tài nguyên

Mỗi job (dù MapReduce hay Spark) đều gồm 2 phần: **thành phần điều phối** + **thành phần xử lý**. Ta cấp sao cho tổng của cả hai bằng nhau:

| Thành phần | MapReduce | Spark | Ghi chú |
|------------|-----------|-------|---------|
| Điều phối | ApplicationMaster: **2048 MB** | Driver: **2048 MB** | Quản lý job |
| Xử lý | Map 3072 + Reduce 3072 = **6144 MB** | Executor 5120 + overhead 1024 = **6144 MB** | Nơi tính toán thực tế |
| vCores | 2 (map) + 2 (reduce) = **4** | Executor: **4** | Số nhân CPU |
| **TỔNG RAM** | **~8192 MB (8GB)** | **~8192 MB (8GB)** | |

> **Lưu ý về "8GB":** YARN được cấp tổng 12GB để có dư cho nhiều container chạy đồng thời.
> Mỗi job tiêu thụ ~8GB (điều phối 2GB + xử lý 6GB). Con số 8GB là **giới hạn tối đa
> mỗi container** đảm bảo job có đủ chỗ chạy mà không bị treo `ACCEPTED`.

---

## 1. yarn-site.xml (nền chung cho cả hai)

Đường dẫn: `$HADOOP_HOME/etc/hadoop/yarn-site.xml`

```xml
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

    <!-- Tổng RAM YARN quản lý: 12GB -->
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>12288</value>
    </property>
    <!-- RAM tối đa cấp cho 1 container: 8GB -->
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>8192</value>
    </property>
    <!-- RAM tối thiểu cấp cho 1 container -->
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>512</value>
    </property>

    <!-- Tổng vCores YARN quản lý -->
    <property>
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>8</value>
    </property>
    <!-- vCores tối đa cấp cho 1 container -->
    <property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>4</value>
    </property>

    <!-- Tắt kiểm tra bộ nhớ để tránh container bị kill -->
    <property>
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>
```

---

## 2. mapred-site.xml (giới hạn RAM/CPU cho MapReduce)

Đường dẫn: `$HADOOP_HOME/etc/hadoop/mapred-site.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hdoop/hadoop-3.3.6</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hdoop/hadoop-3.3.6</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/home/hdoop/hadoop-3.3.6</value>
    </property>

    <!-- ApplicationMaster: 2GB, 1 vCore -->
    <property>
        <name>yarn.app.mapreduce.am.resource.mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.resource.cpu-vcores</name>
        <value>1</value>
    </property>

    <!-- Map task: 3GB, 2 vCores -->
    <property>
        <name>mapreduce.map.memory.mb</name>
        <value>3072</value>
    </property>
    <property>
        <name>mapreduce.map.cpu.vcores</name>
        <value>2</value>
    </property>

    <!-- Reduce task: 3GB, 2 vCores -->
    <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>3072</value>
    </property>
    <property>
        <name>mapreduce.reduce.cpu.vcores</name>
        <value>2</value>
    </property>
</configuration>
```

**Tổng MapReduce dùng:** AM 2048 + Map 3072 + Reduce 3072 = **8192 MB (8GB)**, 4 vCores (map + reduce dùng chung 4 core).

---

## 3. spark-defaults.conf (giới hạn RAM/CPU cho Spark)

Đường dẫn: `$SPARK_HOME/conf/spark-defaults.conf`

```properties
spark.master                     yarn
spark.submit.deployMode          client

# Driver: 2GB (tương đương ApplicationMaster của MapReduce)
spark.driver.memory              2g
spark.driver.cores               1

# Executor: 5GB + 1GB overhead = 6GB, 4 vCores
spark.executor.memory            5g
spark.executor.memoryOverhead    1024
spark.executor.cores             4
spark.executor.instances         1

# Ghi log để xem lại trên History Server
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://localhost:9000/spark-logs
spark.history.fs.logDirectory    hdfs://localhost:9000/spark-logs
```

**Tổng Spark dùng:** Driver 2048 + Executor (5120 + 1024 overhead = 6144) = **8192 MB (8GB)**, 4 vCores.

---

## 4. Lệnh chạy (đã khớp cấu hình)

### MapReduce

```bash
cd /home/hdoop/tungdv
hdfs dfs -rm -r /user/hdoop/prime/output 2>/dev/null

time hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -input /user/hdoop/prime/input/numbers_1gb.txt \
  -output /user/hdoop/prime/output \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -file mapper.py -file reducer.py
```

### Spark

Vì đã ghi sẵn trong `spark-defaults.conf`, chỉ cần:

```bash
cd /home/hdoop/tungdv
time spark-submit prime_spark.py
```

Hoặc chỉ định tường minh trên dòng lệnh (ghi đè config):

```bash
time spark-submit --master yarn --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 5g \
  --conf spark.executor.memoryOverhead=1024 \
  --executor-cores 4 \
  --num-executors 1 \
  prime_spark.py
```

---

## 5. Áp dụng thay đổi

### Cách 1: Dùng script tự động (khuyến nghị)

```bash
bash setup_hadoop_spark.sh update    # Ghi cấu hình mới
bash setup_hadoop_spark.sh restart   # Áp dụng (dừng + khởi động lại)
```

### Cách 2: Thủ công

Sau khi sửa 3 file trên:

```bash
# Khởi động lại YARN để nạp cấu hình mới
stop-yarn.sh
start-yarn.sh

# Kiểm tra tài nguyên đã cập nhật
yarn node -list -showDetails
```

Hoặc xem trên Web UI:

```
http://localhost:8088/cluster/nodes
```

Cột **Mem Avail** phải hiện **12GB**, **VCores Avail** hiện **8**.

---

## 6. Bảng kiểm tra công bằng

| Tiêu chí | MapReduce | Spark | Khớp? |
|----------|-----------|-------|:-----:|
| RAM điều phối | 2048 MB (AM) | 2048 MB (Driver) | ✅ |
| RAM xử lý | 6144 MB (Map+Reduce) | 6144 MB (Executor+overhead) | ✅ |
| Tổng RAM | 8192 MB | 8192 MB | ✅ |
| vCores | 4 | 4 | ✅ |
| Input | numbers_1gb.txt (HDFS) | numbers_1gb.txt (HDFS) | ✅ |
| Giới hạn container YARN | 8GB / 4 vCores | 8GB / 4 vCores | ✅ |

---

## 7. Lưu ý khi đo tốc độ

1. **Chạy mỗi bên 2-3 lần**, lấy trung bình (lần đầu chậm do cache lạnh).
2. Đọc dòng **`real`** trong output của `time` → đó là thời gian thực tế.
3. Kết quả 2 bên (số lượng số nguyên tố) phải **giống nhau** → xác nhận cùng xử lý đúng.
4. Nếu job treo ở trạng thái `ACCEPTED` trên YARN UI → thiếu RAM, kiểm tra lại `yarn.nodemanager.resource.memory-mb`.
5. Không chạy MapReduce và Spark **cùng lúc** — chúng sẽ tranh tài nguyên, làm sai kết quả đo.

---

## 8. So sánh với cấu hình cũ (4GB)

| | Cấu hình cũ (4GB/2core) | Cấu hình mới (8GB/4core) |
|--|------------------------|--------------------------|
| Máy | 4 core / 8GB | 8 core / 16GB |
| YARN tổng | 4GB | 12GB |
| Max container | 4GB / 2 vCores | 8GB / 4 vCores |
| AM / Driver | 1GB | 2GB |
| Xử lý | 2GB | 6GB |
| Tốc độ job | Chậm hơn | Nhanh hơn (nhiều RAM/CPU hơn) |
