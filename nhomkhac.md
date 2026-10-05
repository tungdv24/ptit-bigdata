Thiết lập cấu hình Hadoop
Cấu hình Hadoop được thực hiện thông qua một số file chính. Mỗi file đảm nhiệm một phần khác nhau trong việc thiết lập môi trường và các thành phần của Hadoop.
~/.bashrc: Đây là file cấu hình môi trường của Linux dành cho người dùng. File này được thực thi khi người dùng mở Terminal hoặc đăng nhập. Trong quá trình cài Hadoop, thường dùng để khai báo các biến môi trường như JAVA_HOME, HADOOP_HOME, HADOOP_CONF_DIR và cập nhật PATH, giúp hệ điều hành có thể tìm thấy các lệnh của Hadoop. 
hadoop-env.sh: Đây là file cấu hình môi trường chạy của Hadoop. File này dùng để khai báo các biến môi trường mà Hadoop cần trong quá trình hoạt động, quan trọng nhất là đường dẫn đến Java thông qua biến JAVA_HOME. Nhờ đó Hadoop biết sử dụng phiên bản Java nào để chạy. 
core-site.xml: Đây là file cấu hình cốt lõi của Hadoop. File này xác định các thiết lập chung cho toàn bộ hệ thống Hadoop, đặc biệt là địa chỉ và cổng của hệ thống file mặc định (fs.defaultFS). Ví dụ, nếu cấu hình hdfs://localhost:9000, Hadoop sẽ sử dụng HDFS trên máy hiện tại thông qua cổng 9000. 
hdfs-site.xml: Đây là file cấu hình HDFS (Hadoop Distributed File System). File này quy định cách Hadoop lưu trữ và quản lý dữ liệu, chẳng hạn như thư mục lưu NameNode, thư mục lưu DataNode và số lượng bản sao dữ liệu thông qua dfs.replication. 
mapred-site.xml: Đây là file cấu hình MapReduce, framework dùng để xử lý dữ liệu trong Hadoop. File này xác định cách các công việc MapReduce được thực thi. Ví dụ, có thể cấu hình MapReduce chạy trên YARN thông qua thuộc tính mapreduce.framework.name. 
yarn-site.xml: Đây là file cấu hình YARN (Yet Another Resource Negotiator). YARN chịu trách nhiệm quản lý tài nguyên và điều phối các ứng dụng chạy trên Hadoop. File này thường cấu hình ResourceManager, NodeManager và các dịch vụ hỗ trợ MapReduce như mapreduce_shuffle. 
 



Mối quan hệ giữa các file như sau:
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
│  core-site.xml → Cấu hình chung      │
│  hdfs-site.xml → Lưu trữ HDFS        │
│  mapred-site.xml → Xử lý MapReduce   │
│  yarn-site.xml → Quản lý tài nguyên  │
└──────────────────────────────────────┘

Thiết lập các biến môi trường cho Hadoop
Các biến môi trường giúp hệ điều hành và Hadoop xác định vị trí cài đặt Hadoop, đồng thời cho phép sử dụng các lệnh Hadoop từ bất kỳ thư mục nào trong Terminal.
Mở file .bashrc của người dùng:
sudo nano ~/.bashrc
Bổ sung các dòng sau vào cuối file:
# Hadoop Related Options
export HADOOP_HOME=/home/hdoop/hadoop-3.2.2
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
Trong đó:
HADOOP_HOME: xác định thư mục cài đặt Hadoop. 
HADOOP_INSTALL: trỏ đến thư mục cài đặt Hadoop. 
HADOOP_MAPRED_HOME: xác định thư mục chứa các thành phần MapReduce. 
HADOOP_COMMON_HOME: xác định các thành phần thư viện chung của Hadoop. 
HADOOP_HDFS_HOME: xác định các thành phần liên quan đến HDFS. 
YARN_HOME: xác định các thành phần liên quan đến YARN. 
HADOOP_COMMON_LIB_NATIVE_DIR: xác định vị trí các thư viện native của Hadoop. 
PATH: thêm thư mục bin và sbin của Hadoop vào PATH, giúp có thể chạy các lệnh như hadoop, hdfs, start-dfs.sh,... trực tiếp từ Terminal. 
HADOOP_OPTS: khai báo đường dẫn để Java có thể tìm thấy các thư viện native của Hadoop. 
Sau khi thêm các cấu hình, nhấn Ctrl + X, chọn Y để lưu và nhấn Enter để đóng file.
Thực hiện lệnh sau để áp dụng các thay đổi vào môi trường hiện tại:
source ~/.bashrc
Sau đó có thể kiểm tra biến HADOOP_HOME:
echo $HADOOP_HOME
Nếu kết quả trả về:
/home/hdoop/hadoop-3.2.2
thì biến môi trường HADOOP_HOME đã được thiết lập thành công.

Thiết lập cấu hình Hadoop
File hadoop-env.sh chứa các thiết lập môi trường chạy của Hadoop, trong đó quan trọng nhất là khai báo đường dẫn đến Java để Hadoop có thể sử dụng Java khi khởi động và thực thi các thành phần như HDFS, YARN và MapReduce.
Mở file hadoop-env.sh:
sudo nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
Tìm phần khai báo JAVA_HOME và bổ sung đường dẫn đến thư mục cài đặt Java:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
Trong đó, JAVA_HOME phải trỏ đến thư mục cài đặt Java, không phải trực tiếp đến file java hoặc javac.

Lưu ý: Để xác định đường dẫn đến chương trình Java Compiler (javac), sử dụng lệnh:
which javac
Ví dụ kết quả:
/usr/bin/javac
Do /usr/bin/javac thường là một symbolic link, sử dụng lệnh sau để xác định đường dẫn thực tế:
readlink -f /usr/bin/javac
Ví dụ kết quả:
/usr/lib/jvm/java-8-openjdk-amd64/bin/javac
Từ kết quả trên, xác định JAVA_HOME bằng cách lấy phần thư mục trước /bin/javac:
/usr/lib/jvm/java-8-openjdk-amd64
Sau đó khai báo:
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
Sau khi lưu file, có thể kiểm tra lại cấu hình bằng:
echo $JAVA_HOME
Nếu kết quả hiển thị đúng đường dẫn Java thì JAVA_HOME đã được thiết lập thành công.

Thiết lập cấu hình Hadoop core
Trước khi cấu hình Hadoop, cần tạo thư mục tạm để Hadoop lưu trữ các file trung gian trong quá trình hoạt động.
Tạo thư mục /app/hadoop/tmp:
sudo mkdir -p /app/hadoop/tmp
sudo chown hdoop:hdoop /app/hadoop/tmp
sudo chmod 750 /app/hadoop/tmp
Trong đó:
mkdir -p: tạo thư mục /app/hadoop/tmp. 
chown hdoop:hdoop: cấp quyền sở hữu thư mục cho user và group hdoop. 
chmod 750: cho phép chủ sở hữu đọc, ghi, thực thi; group được đọc và thực thi; các user khác không có quyền. 
Để thiết lập Hadoop ở chế độ giả lập phân tán (Pseudo-Distributed Mode), cần cấu hình core-site.xml. File này chứa các thiết lập chung của Hadoop, trong đó xác định thư mục tạm và HDFS mặc định.
Mở file core-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/core-site.xml
Bổ sung các khai báo sau:
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
Trong đó:
hadoop.tmp.dir: xác định thư mục tạm mà Hadoop sử dụng để lưu các file trung gian. 
fs.defaultFS: xác định hệ thống file mặc định của Hadoop. hdfs://localhost:9000 nghĩa là Hadoop sử dụng HDFS trên chính máy hiện tại, với NameNode chạy tại cổng 9000. 

Cấu hình HDFS
File hdfs-site.xml dùng để cấu hình HDFS, hệ thống file phân tán của Hadoop. File này quy định cách HDFS lưu trữ và quản lý dữ liệu.
Mở file hdfs-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
Bổ sung:
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
Trong đó:
dfs.replication: quy định số lượng bản sao của mỗi block dữ liệu trên HDFS. 
Giá trị 1 nghĩa là mỗi block dữ liệu chỉ có một bản sao. 
Thiết lập này phù hợp với môi trường Pseudo-Distributed Mode chạy trên một máy, vì không có nhiều DataNode để tạo các bản sao dữ liệu.

Cấu hình MapReduce
MapReduce là framework dùng để xử lý dữ liệu trong Hadoop. File mapred-site.xml xác định cách các chương trình MapReduce được thực thi.
Mở file:
sudo nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
Bổ sung:
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>

    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
Trong đó:
mapreduce.framework.name: xác định framework mà MapReduce sử dụng. Giá trị yarn nghĩa là các job MapReduce được quản lý và thực thi thông qua YARN. 
mapreduce.application.classpath: xác định đường dẫn đến các thư viện cần thiết để ứng dụng MapReduce có thể chạy. 
Có thể hình dung luồng xử lý:
MapReduce Job
      ↓
     YARN
      ↓
  ResourceManager
      ↓
   NodeManager
      ↓
Thực thi Map / Reduce

Thiết lập YARN
YARN (Yet Another Resource Negotiator) chịu trách nhiệm quản lý tài nguyên và điều phối các ứng dụng chạy trên Hadoop.
Mở file yarn-site.xml:
sudo nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
Bổ sung:
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
Trong đó:
yarn.nodemanager.aux-services: khai báo các dịch vụ hỗ trợ chạy trên NodeManager. mapreduce_shuffle là dịch vụ hỗ trợ quá trình Shuffle, tức là trao đổi và sắp xếp dữ liệu giữa Map và Reduce. 
yarn.nodemanager.env-whitelist: cho phép các biến môi trường cần thiết như JAVA_HOME, HADOOP_HOME, HADOOP_MAPRED_HOME,... được truyền vào môi trường thực thi của NodeManager. 

Định dạng HDFS
Sau khi hoàn thành cấu hình, trước khi sử dụng HDFS lần đầu tiên cần định dạng NameNode.
Thực hiện lệnh:
hdfs namenode -format
Quá trình này sẽ khởi tạo các metadata và cấu trúc thư mục cần thiết cho NameNode để bắt đầu quản lý HDFS.
Nếu định dạng thành công, kết quả thường xuất hiện thông báo tương tự:
Storage directory ... has been successfully formatted.
Lưu ý: Lệnh hdfs namenode -format chỉ nên thực hiện khi khởi tạo HDFS lần đầu. Không nên chạy lại trên một HDFS đang sử dụng vì có thể làm mất metadata và khiến dữ liệu HDFS hiện tại không còn được nhận diện