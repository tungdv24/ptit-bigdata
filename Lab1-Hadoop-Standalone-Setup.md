# Lab 1: Cài đặt Hadoop chế độ Pseudo-Distributed (Standalone)

> Hướng dẫn cài đặt Hadoop trên một máy Ubuntu duy nhất (pseudo-distributed mode).
> Phù hợp cho mục đích học tập và thực hành trong phòng lab.

---

## Mục lục

1. [Giới thiệu](#giới-thiệu)
2. [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
3. [Thiết lập môi trường](#thiết-lập-môi-trường)
4. [Cài đặt Hadoop](#cài-đặt-hadoop)
5. [Cấu hình Hadoop](#cấu-hình-hadoop)
6. [Định dạng HDFS và Khởi động](#định-dạng-hdfs-và-khởi-động)
7. [Cài đặt Spark](#cài-đặt-spark)
8. [Kiểm tra hệ thống](#kiểm-tra-hệ-thống)
9. [Các lệnh quản trị](#các-lệnh-quản-trị)
10. [Xử lý lỗi thường gặp](#xử-lý-lỗi-thường-gặp)

---

## Giới thiệu

Hadoop là nền tảng mã nguồn mở dùng để xây dựng hệ thống xử lý dữ liệu lớn (Big Data) theo mô hình phân tán. Hadoop có 3 chế độ triển khai:

| Chế độ | Mô tả |
|--------|--------|
| **Standalone** | Mọi tiến trình chạy trên 1 JVM, dùng để debug |
| **Pseudo-distributed** | NameNode và DataNode chạy trên cùng 1 máy |
| **Fully distributed** | Các tiến trình chạy trên nhiều máy vật lý |

Bài lab này cài đặt Hadoop ở chế độ **Pseudo-distributed** — mọi thành phần (NameNode, DataNode, ResourceManager, NodeManager) đều chạy trên một máy duy nhất.

---

## Yêu cầu hệ thống

| Thành phần | Yêu cầu |
|------------|----------|
| Hệ điều hành | Ubuntu 20.04 / 22.04 / 24.04 LTS |
| RAM | Tối thiểu 4 GB (khuyến nghị 8 GB) |
| Ổ đĩa | Tối thiểu 20 GB trống |
| Java | OpenJDK 11 (hoặc OpenJDK 8) |
| Hadoop | 3.3.6 |
| Spark | 3.5.9 |

> Có thể chạy Ubuntu trên VirtualBox nếu đang sử dụng Windows.

---

## Thiết lập môi trường

### 1. Cập nhật hệ thống

```bash
sudo apt update && sudo apt upgrade -y
```

### 2. Tạo tài khoản quản trị Hadoop

```bash
sudo adduser hdoop
```

Nhập mật khẩu khi được yêu cầu.

Cấp quyền sudo cho tài khoản:

```bash
sudo usermod -aG sudo hdoop
```

Đăng nhập vào tài khoản hdoop:

```bash
su - hdoop
```

### 3. Cài đặt SSH

```bash
sudo apt install openssh-server openssh-client -y
```

Tạo cặp khóa SSH (để đăng nhập localhost không cần mật khẩu):

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

Kiểm tra SSH:

```bash
ssh localhost
```

Nhập `yes` khi được hỏi. Nếu đăng nhập thành công mà không cần mật khẩu → SSH đã cài đặt đúng.

Gõ `exit` để thoát phiên SSH.

### 4. Tắt IPv6 (khuyến nghị)

```bash
sudo nano /etc/sysctl.conf
```

Thêm vào cuối file:

```
# Disable IPv6
net.ipv6.conf.all.disable_ipv6 = 1
net.ipv6.conf.default.disable_ipv6 = 1
net.ipv6.conf.lo.disable_ipv6 = 1
```

Áp dụng:

```bash
sudo sysctl -p
```

Kiểm tra:

```bash
cat /proc/sys/net/ipv6/conf/all/disable_ipv6
# Kết quả: 1 (IPv6 đã tắt)
```

### 5. Cài đặt Java

```bash
sudo apt install openjdk-11-jdk -y
```

Kiểm tra:

```bash
java -version
# openjdk version "11.0.x"
```

Xác định đường dẫn Java:

```bash
readlink -f $(which javac) | sed 's|/bin/javac||'
# Kết quả: /usr/lib/jvm/java-11-openjdk-amd64
```

---

## Cài đặt Hadoop

### 1. Tải Hadoop 3.3.6

```bash
cd ~
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
```

### 2. Giải nén

```bash
tar xzf hadoop-3.3.6.tar.gz
```

### 3. Thiết lập biến môi trường

Mở file `.bashrc`:

```bash
nano ~/.bashrc
```

Thêm vào cuối file:

```bash
# Java
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Hadoop
export HADOOP_HOME=/home/hdoop/hadoop-3.3.6
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
```

Áp dụng:

```bash
source ~/.bashrc
```

### 4. Kiểm tra cài đặt

```bash
hadoop version
```

Kết quả mong đợi:

```
Hadoop 3.3.6
```

---

## Cấu hình Hadoop

### Tổng quan các file cấu hình

Cấu hình Hadoop được thực hiện thông qua một số file chính, mỗi file đảm nhiệm một phần khác nhau trong việc thiết lập môi trường và các thành phần của Hadoop:

| File | Vai trò |
|------|---------|
| `~/.bashrc` | File cấu hình môi trường Linux của người dùng, được thực thi khi mở Terminal hoặc đăng nhập. Dùng để khai báo các biến môi trường như `JAVA_HOME`, `HADOOP_HOME` và cập nhật `PATH`, giúp hệ điều hành tìm thấy các lệnh Hadoop. |
| `hadoop-env.sh` | File cấu hình môi trường chạy của Hadoop. Khai báo các biến môi trường Hadoop cần khi hoạt động, quan trọng nhất là đường dẫn Java (`JAVA_HOME`). |
| `core-site.xml` | File cấu hình cốt lõi. Xác định các thiết lập chung cho toàn bộ hệ thống, đặc biệt là địa chỉ và cổng của hệ thống file mặc định (`fs.defaultFS`). |
| `hdfs-site.xml` | File cấu hình HDFS. Quy định cách Hadoop lưu trữ và quản lý dữ liệu: thư mục NameNode, thư mục DataNode, số bản sao dữ liệu (`dfs.replication`). |
| `mapred-site.xml` | File cấu hình MapReduce. Xác định cách các công việc MapReduce được thực thi (ví dụ chạy trên YARN). |
| `yarn-site.xml` | File cấu hình YARN. Quản lý tài nguyên và điều phối các ứng dụng, cấu hình ResourceManager, NodeManager và dịch vụ `mapreduce_shuffle`. |

**Mối quan hệ giữa các file:**

```
~/.bashrc
    ↓
Thiết lập môi trường Linux
    ↓
hadoop-env.sh
    ↓
Thiết lập môi trường chạy Hadoop
    ↓
┌──────────────────────────────────────┐
│           Hadoop Cluster             │
│                                      │
│  core-site.xml  → Cấu hình chung     │
│  hdfs-site.xml  → Lưu trữ HDFS       │
│  mapred-site.xml → Xử lý MapReduce   │
│  yarn-site.xml  → Quản lý tài nguyên │
└──────────────────────────────────────┘
```

> **Về các biến môi trường trong `.bashrc`** (đã cấu hình ở phần "Cài đặt Hadoop" phía trên):
> - `HADOOP_HOME`: thư mục cài đặt Hadoop
> - `HADOOP_INSTALL`: trỏ đến thư mục cài đặt Hadoop
> - `HADOOP_MAPRED_HOME`: thư mục chứa các thành phần MapReduce
> - `HADOOP_COMMON_HOME`: các thành phần thư viện chung của Hadoop
> - `HADOOP_HDFS_HOME`: các thành phần liên quan đến HDFS
> - `HADOOP_YARN_HOME`: các thành phần liên quan đến YARN
> - `HADOOP_COMMON_LIB_NATIVE_DIR`: vị trí các thư viện native
> - `PATH`: thêm `bin` và `sbin` của Hadoop để chạy được lệnh `hadoop`, `hdfs`, `start-dfs.sh`... từ mọi thư mục
> - `HADOOP_OPTS`: khai báo đường dẫn để Java tìm thấy thư viện native
>
> Kiểm tra biến đã được thiết lập chưa:
> ```bash
> echo $HADOOP_HOME
> # Kết quả mong đợi: /home/hdoop/hadoop-3.3.6
> ```

---

### 1. hadoop-env.sh

File `hadoop-env.sh` chứa các thiết lập môi trường chạy của Hadoop, trong đó quan trọng nhất là khai báo đường dẫn đến Java để Hadoop có thể sử dụng Java khi khởi động và thực thi các thành phần như HDFS, YARN và MapReduce.

```bash
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

Tìm phần khai báo `JAVA_HOME` và thêm dòng:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

> **Lưu ý quan trọng:** `JAVA_HOME` phải trỏ đến **thư mục cài đặt Java**, không phải trực tiếp đến file `java` hoặc `javac`.
>
> Cách xác định đường dẫn chính xác:
> ```bash
> which javac
> # Ví dụ: /usr/bin/javac
>
> # Vì /usr/bin/javac thường là symbolic link, tìm đường dẫn thực:
> readlink -f /usr/bin/javac
> # Ví dụ: /usr/lib/jvm/java-11-openjdk-amd64/bin/javac
> ```
> Lấy phần thư mục trước `/bin/javac` → đó chính là `JAVA_HOME`.
>
> Kiểm tra sau khi lưu:
> ```bash
> echo $JAVA_HOME
> ```

### 2. Tạo thư mục dữ liệu

Trước khi cấu hình, cần tạo các thư mục để Hadoop lưu trữ metadata (NameNode), dữ liệu (DataNode) và các file trung gian trong quá trình hoạt động.

```bash
sudo mkdir -p /app/hadoop/tmp
sudo mkdir -p /app/hadoop/hdfs/namenode
sudo mkdir -p /app/hadoop/hdfs/datanode
sudo chown -R hdoop:hdoop /app/hadoop
sudo chmod -R 750 /app/hadoop
```

Giải thích các lệnh:

- `mkdir -p`: tạo các thư mục (kèm thư mục cha nếu chưa tồn tại)
- `chown -R hdoop:hdoop`: cấp quyền sở hữu thư mục cho user và group `hdoop`
- `chmod -R 750`: chủ sở hữu được đọc/ghi/thực thi; group được đọc/thực thi; user khác không có quyền

### 3. core-site.xml

Để thiết lập Hadoop ở chế độ giả lập phân tán (Pseudo-Distributed Mode), cần cấu hình `core-site.xml`. File này chứa các thiết lập chung của Hadoop, trong đó xác định thư mục tạm và hệ thống file mặc định (HDFS).

```bash
nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

Nội dung:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/app/hadoop/tmp</value>
    </property>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

Giải thích các thuộc tính:

| Thuộc tính | Giá trị | Ý nghĩa |
|------------|---------|---------|
| `hadoop.tmp.dir` | `/app/hadoop/tmp` | Xác định thư mục tạm mà Hadoop sử dụng để lưu các file trung gian trong quá trình hoạt động |
| `fs.defaultFS` | `hdfs://localhost:9000` | Xác định hệ thống file mặc định của Hadoop. Giá trị này nghĩa là Hadoop sử dụng HDFS ngay trên máy hiện tại, với NameNode chạy tại cổng 9000 |

### 4. hdfs-site.xml

File `hdfs-site.xml` dùng để cấu hình HDFS (Hadoop Distributed File System) — hệ thống file phân tán của Hadoop. HDFS chia một file lớn thành các block và lưu trên nhiều máy, đảm bảo khả năng lưu trữ lớn và chịu lỗi cao.

**Kiến trúc HDFS:**

```
              ┌─────────────────────────┐
              │       NameNode          │
              │  (quản lý metadata:     │
              │   file, block, vị trí)  │
              └───────────┬─────────────┘
                          │
          ┌───────────────┼───────────────┐
          ↓               ↓               ↓
   ┌────────────┐  ┌────────────┐  ┌────────────┐
   │ DataNode 1 │  │ DataNode 2 │  │ DataNode N │
   │ (lưu block │  │ (lưu block │  │ (lưu block │
   │  dữ liệu)  │  │  dữ liệu)  │  │  dữ liệu)  │
   └────────────┘  └────────────┘  └────────────┘
```

| Thành phần | Vai trò |
|------------|---------|
| **NameNode** | Quản lý metadata — thông tin về cấu trúc thư mục, tên file, và vị trí các block. Không lưu dữ liệu thực tế |
| **DataNode** | Lưu trữ dữ liệu thực tế (các block). Định kỳ báo cáo trạng thái về NameNode |
| **SecondaryNameNode** | Hỗ trợ NameNode tạo checkpoint cho metadata (không phải bản backup) |

> Ở chế độ Pseudo-Distributed, cả NameNode và DataNode cùng chạy trên một máy.

```bash
nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

Nội dung:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///app/hadoop/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///app/hadoop/hdfs/datanode</value>
    </property>
</configuration>
```

Giải thích các thuộc tính:

| Thuộc tính | Giá trị | Ý nghĩa |
|------------|---------|---------|
| `dfs.replication` | `1` | Quy định số lượng bản sao của mỗi block dữ liệu trên HDFS. Giá trị `1` nghĩa là mỗi block chỉ có một bản sao — phù hợp với Pseudo-Distributed Mode chạy trên một máy, vì không có nhiều DataNode để tạo bản sao |
| `dfs.namenode.name.dir` | `file:///app/hadoop/hdfs/namenode` | Thư mục lưu metadata của NameNode (thông tin về cấu trúc file, block) |
| `dfs.datanode.data.dir` | `file:///app/hadoop/hdfs/datanode` | Thư mục lưu dữ liệu thực tế (các block) của DataNode |

### 5. mapred-site.xml

MapReduce là mô hình lập trình và framework dùng để xử lý dữ liệu lớn theo cách song song trong Hadoop. Quá trình xử lý gồm 2 pha chính: **Map** (xử lý và tạo cặp key-value) và **Reduce** (tổng hợp kết quả). File `mapred-site.xml` xác định cách các chương trình MapReduce được thực thi.

**Kiến trúc MapReduce:**

```
Input → [Map] → (key, value) → [Shuffle & Sort] → [Reduce] → Output
```

| Pha | Vai trò |
|-----|---------|
| **Map** | Đọc dữ liệu đầu vào, xử lý từng phần và tạo ra các cặp (key, value) trung gian |
| **Shuffle & Sort** | Gom nhóm và sắp xếp các value có cùng key (Hadoop tự thực hiện) |
| **Reduce** | Nhận các nhóm (key, [values]) và tổng hợp ra kết quả cuối cùng |

```bash
nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
```

Nội dung:

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
</configuration>
```

Giải thích các thuộc tính:

| Thuộc tính | Giá trị | Ý nghĩa |
|------------|---------|---------|
| `mapreduce.framework.name` | `yarn` | Xác định framework mà MapReduce sử dụng. Giá trị `yarn` nghĩa là các job MapReduce được quản lý và thực thi thông qua YARN |
| `mapreduce.application.classpath` | `$HADOOP_MAPRED_HOME/share/...` | Xác định đường dẫn đến các thư viện cần thiết để ứng dụng MapReduce có thể chạy |
| `yarn.app.mapreduce.am.env` | `HADOOP_MAPRED_HOME=...` | Khai báo biến `HADOOP_MAPRED_HOME` cho tiến trình ApplicationMaster |
| `mapreduce.map.env` | `HADOOP_MAPRED_HOME=...` | Khai báo biến `HADOOP_MAPRED_HOME` cho tiến trình Map |
| `mapreduce.reduce.env` | `HADOOP_MAPRED_HOME=...` | Khai báo biến `HADOOP_MAPRED_HOME` cho tiến trình Reduce |

Luồng xử lý một job MapReduce:

```
MapReduce Job
      ↓
     YARN
      ↓
  ResourceManager
      ↓
   NodeManager
      ↓
Thực thi Map / Reduce
```

### 6. yarn-site.xml

YARN (Yet Another Resource Negotiator) chịu trách nhiệm quản lý tài nguyên và điều phối các ứng dụng chạy trên Hadoop. YARN đóng vai trò như "người quản lý tài nguyên" của cluster: khi một job (MapReduce hoặc Spark) được gửi lên, YARN quyết định cấp bao nhiêu RAM/CPU, chạy trên node nào, và điều phối các tiến trình chạy song song.

**Kiến trúc YARN:**

```
Job (MapReduce / Spark)
        ↓
      YARN
        ↓
┌────────────────────────────────────────────┐
│  ResourceManager (quản lý tài nguyên chung) │
│           ↓                                  │
│  NodeManager (quản lý từng máy)             │
│           ↓                                  │
│  Container (cấp CPU + RAM để chạy task)     │
└────────────────────────────────────────────┘
```

| Thành phần | Vai trò |
|------------|---------|
| **ResourceManager** | Chạy trên máy master, quản lý và cấp phát tài nguyên cho toàn bộ cluster |
| **NodeManager** | Chạy trên từng máy, quản lý tài nguyên của máy đó và khởi tạo container |
| **Container** | Đơn vị tài nguyên (CPU + RAM) được cấp để thực thi một task |

```bash
nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
```

Nội dung:

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
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>2048</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>2048</value>
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
```

Giải thích các thuộc tính:

| Thuộc tính | Giá trị | Ý nghĩa |
|------------|---------|---------|
| `yarn.nodemanager.aux-services` | `mapreduce_shuffle` | Khai báo dịch vụ hỗ trợ chạy trên NodeManager. `mapreduce_shuffle` là dịch vụ hỗ trợ quá trình Shuffle — trao đổi và sắp xếp dữ liệu giữa pha Map và pha Reduce. Bắt buộc phải có, nếu không job MapReduce sẽ lỗi |
| `yarn.nodemanager.env-whitelist` | `JAVA_HOME,...` | Cho phép các biến môi trường cần thiết (`JAVA_HOME`, `HADOOP_HOME`, `HADOOP_MAPRED_HOME`...) được truyền vào môi trường thực thi của container trên NodeManager |
| `yarn.nodemanager.resource.memory-mb` | `2048` | Tổng bộ nhớ (MB) mà NodeManager được phép sử dụng để cấp cho các container |
| `yarn.scheduler.maximum-allocation-mb` | `2048` | Bộ nhớ tối đa (MB) cấp cho mỗi container. Giá trị 2048 MB phù hợp với máy 4 GB RAM |
| `yarn.nodemanager.pmem-check-enabled` | `false` | Tắt kiểm tra bộ nhớ **vật lý** (physical memory) |
| `yarn.nodemanager.vmem-check-enabled` | `false` | Tắt kiểm tra bộ nhớ **ảo** (virtual memory) |

> **Vì sao tắt kiểm tra bộ nhớ (`pmem-check` và `vmem-check`)?**
> Hai thiết lập này đặt `false` để tránh việc YARN tự động **kill container** khi vượt ngưỡng bộ nhớ. Trên máy lab RAM thấp (4 GB), lỗi này rất hay xảy ra và làm job bị dừng đột ngột.

> **Liên hệ với lỗi thường gặp khi chạy Spark:**
> Cảnh báo `Initial job has not accepted any resources` khi chạy Spark trên YARN xuất phát từ việc YARN không đủ tài nguyên để cấp cho job — được điều khiển bởi `yarn.nodemanager.resource.memory-mb` trong file này. Nếu job bị treo ở trạng thái `ACCEPTED`, có thể tăng giá trị này (ví dụ `3072`) hoặc giảm bộ nhớ yêu cầu của Spark.

---

## Định dạng HDFS và Khởi động

### 1. Định dạng NameNode (chỉ chạy 1 lần duy nhất)

```bash
hdfs namenode -format
```

Kết quả cuối cùng phải có: `Storage directory ... has been successfully formatted.`

### 2. Khởi động HDFS

```bash
start-dfs.sh
```

### 3. Khởi động YARN

```bash
start-yarn.sh
```

### 4. Kiểm tra các tiến trình

```bash
jps
```

Kết quả mong đợi (5 tiến trình):

```
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

### 5. Truy cập giao diện Web

| Dịch vụ | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |

---

## Cài đặt Spark

### 1. Tải Spark 3.5.9

```bash
cd ~
wget https://dlcdn.apache.org/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz
```

### 2. Giải nén

```bash
tar xzf spark-3.5.9-bin-hadoop3.tgz
```

### 3. Thêm biến môi trường

```bash
nano ~/.bashrc
```

Thêm vào cuối file:

```bash
# Spark
export SPARK_HOME=/home/hdoop/spark-3.5.9-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=python3
```

Áp dụng:

```bash
source ~/.bashrc
```

### 4. Cấu hình Spark

Tạo file `spark-env.sh`:

```bash
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
nano $SPARK_HOME/conf/spark -env.sh
```

Thêm vào cuối:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/home/hdoop/hadoop-3.3.6
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PYSPARK_PYTHON=python3
```

Tạo file `spark-defaults.conf`:

```bash
cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
nano $SPARK_HOME/conf/spark-defaults.conf
```

Thêm:

```properties
spark.master                     yarn
spark.submit.deployMode          client
spark.driver.memory              512m
spark.executor.memory            512m
spark.executor.cores             1
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://localhost:9000/spark-logs
spark.history.fs.logDirectory    hdfs://localhost:9000/spark-logs
```

### 5. Tạo thư mục log trên HDFS

```bash
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -chmod 777 /spark-logs
```

### 6. Khởi động Spark History Server

```bash
$SPARK_HOME/sbin/start-history-server.sh
```

Truy cập: http://localhost:18080

### 7. Kiểm tra Spark

```bash
spark-submit --master yarn --deploy-mode client \
  $SPARK_HOME/examples/src/main/python/pi.py 10
```

Kết quả: `Pi is roughly 3.14xxxx`

---

## Kiểm tra hệ thống

### Test HDFS

```bash
# Tạo thư mục trên HDFS
hdfs dfs -mkdir -p /user/hdoop

# Upload file
echo "Hello Hadoop" > /tmp/test.txt
hdfs dfs -put /tmp/test.txt /user/hdoop/

# Đọc file từ HDFS
hdfs dfs -cat /user/hdoop/test.txt

# Liệt kê file
hdfs dfs -ls /user/hdoop/
```

### Test MapReduce trên YARN

```bash
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar pi 2 5
```

### Test PySpark interactive

```bash
pyspark --master yarn
```

```python
# Trong PySpark shell
rdd = sc.parallelize(range(100))
print(rdd.sum())  # Kết quả: 4950
exit()
```

---

## Các lệnh quản trị

### Khởi động toàn bộ

```bash
start-dfs.sh
start-yarn.sh
$SPARK_HOME/sbin/start-history-server.sh
```

Hoặc gộp:

```bash
start-all.sh
$SPARK_HOME/sbin/start-history-server.sh
```

### Dừng toàn bộ

```bash
$SPARK_HOME/sbin/stop-history-server.sh
stop-yarn.sh
stop-dfs.sh
```

Hoặc gộp:

```bash
$SPARK_HOME/sbin/stop-history-server.sh
stop-all.sh
```

### Kiểm tra trạng thái

```bash
jps                           # Liệt kê các tiến trình Java
hdfs dfsadmin -report         # Báo cáo HDFS
yarn node -list               # Danh sách YARN node
```

---

## Xử lý lỗi thường gặp

### Lỗi: `JAVA_HOME is not set`

Kiểm tra lại dòng `export JAVA_HOME=...` trong cả `~/.bashrc` và `hadoop-env.sh`.

### Lỗi: SSH localhost bị từ chối

```bash
sudo service ssh restart
ssh localhost
```

### Lỗi: NameNode không khởi động

Có thể do format sai hoặc dữ liệu cũ:

```bash
# Xóa dữ liệu cũ
rm -rf /app/hadoop/hdfs/namenode/*
rm -rf /app/hadoop/hdfs/datanode/*
rm -rf /app/hadoop/tmp/*

# Format lại
hdfs namenode -format

# Khởi động lại
start-dfs.sh
```

### Lỗi: DataNode không chạy

Kiểm tra ClusterID có khớp giữa NameNode và DataNode:

```bash
cat /app/hadoop/hdfs/namenode/current/VERSION
cat /app/hadoop/hdfs/datanode/current/VERSION
```

Nếu `clusterID` khác nhau → xóa datanode, format lại.

### Lỗi: YARN Container vượt quá bộ nhớ

Thêm vào `yarn-site.xml`:

```xml
<property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
</property>
<property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
</property>
```

### Lỗi: Spark không tìm thấy HADOOP_CONF_DIR

Đảm bảo `spark-env.sh` có dòng:

```bash
export HADOOP_CONF_DIR=/home/hdoop/hadoop-3.3.6/etc/hadoop
```

---

## Tổng hợp các file cấu hình

| File | Đường dẫn | Chức năng |
|------|-----------|-----------|
| `.bashrc` | `~/.bashrc` | Biến môi trường |
| `hadoop-env.sh` | `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` | JAVA_HOME cho Hadoop |
| `core-site.xml` | `$HADOOP_HOME/etc/hadoop/core-site.xml` | HDFS URI, thư mục tmp |
| `hdfs-site.xml` | `$HADOOP_HOME/etc/hadoop/hdfs-site.xml` | Replication, data dirs |
| `mapred-site.xml` | `$HADOOP_HOME/etc/hadoop/mapred-site.xml` | MapReduce framework |
| `yarn-site.xml` | `$HADOOP_HOME/etc/hadoop/yarn-site.xml` | YARN resource config |
| `spark-env.sh` | `$SPARK_HOME/conf/spark-env.sh` | Spark environment |
| `spark-defaults.conf` | `$SPARK_HOME/conf/spark-defaults.conf` | Spark default settings |

---

## Giao diện Web

| Dịch vụ | URL | Mô tả |
|---------|-----|--------|
| HDFS NameNode | http://localhost:9870 | Quản lý file HDFS |
| YARN ResourceManager | http://localhost:8088 | Quản lý jobs |
| Spark History Server | http://localhost:18080 | Lịch sử Spark jobs |
| Spark Application UI | http://localhost:4040 | Job đang chạy |

---

## Cấu trúc thư mục

```
/home/hdoop/
├── hadoop-3.3.6/              # Hadoop installation
│   ├── bin/                   # Hadoop commands
│   ├── sbin/                  # Start/stop scripts
│   └── etc/hadoop/            # Config files
├── spark-3.5.9-bin-hadoop3/   # Spark installation
│   ├── bin/                   # spark-submit, pyspark, spark-sql
│   ├── sbin/                  # History server scripts
│   └── conf/                  # Spark config
└── .bashrc                    # Environment variables

/app/hadoop/
├── tmp/                       # Hadoop temp
└── hdfs/
    ├── namenode/              # NameNode metadata
    └── datanode/              # DataNode blocks
```

---

## Tham khảo

- [Apache Hadoop 3.3.6 Documentation](https://hadoop.apache.org/docs/r3.3.6/)
- [Apache Spark 3.5.x Documentation](https://spark.apache.org/docs/3.5.9/)
- [nd-hung/Big-Data Lab1](https://github.com/nd-hung/Big-Data/tree/main/Lab1_Hadoop_Installation)
- [Running Hadoop On Ubuntu Linux (Single-Node Cluster)](http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/)
