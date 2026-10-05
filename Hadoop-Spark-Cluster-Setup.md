# Hadoop 3.3.6 + Apache Spark 3.5.9 Cluster Setup Guide

> Lab-purpose 3-node cluster on Ubuntu 24.04 LTS

## Cluster Overview

| # | Hostname | IP Address | SSH Port | Role |
|---|----------|------------|----------|------|
| 1 | master | 192.168.10.56 | 2112 | NameNode, SecondaryNameNode, ResourceManager, Spark Master |
| 2 | worker1 | 192.168.10.57 | 2112 | DataNode, NodeManager, Spark Worker |
| 3 | worker2 | 192.168.10.58 | 2112 | DataNode, NodeManager, Spark Worker |

### Server Specifications

| Resource | Value |
|----------|-------|
| OS | Ubuntu 24.04.4 LTS (Noble Numbat) |
| RAM | 4 GB per node |
| CPU | 2 vCPUs per node |
| Disk | 50 GB (master), 30 GB (workers) |

### Software Versions

| Software | Version |
|----------|---------|
| Java | OpenJDK 11.0.31 |
| Hadoop | 3.3.6 |
| Spark | 3.5.9 |
| Scala | 2.12.18 |
| Python | 3.x (for PySpark) |

---

## Step 1: Set Hostnames and /etc/hosts

### Set hostname on each node

```bash
# On master (192.168.10.56)
hostnamectl set-hostname master

# On worker1 (192.168.10.57)
hostnamectl set-hostname worker1

# On worker2 (192.168.10.58)
hostnamectl set-hostname worker2
```

### Configure /etc/hosts (all nodes)

Add the following to `/etc/hosts` on ALL 3 servers:

```
192.168.10.56 master
192.168.10.57 worker1
192.168.10.58 worker2
```

### Disable cloud-init hosts management (if applicable)

```bash
sed -i 's/manage_etc_hosts: true/manage_etc_hosts: false/' /etc/cloud/cloud.cfg
```

---

## Step 2: Install Java (OpenJDK 11)

Run on ALL nodes:

```bash
apt-get update
apt-get install -y openjdk-11-jdk
```

### Set JAVA_HOME globally

Create `/etc/profile.d/java.sh` on all nodes:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

```bash
chmod +x /etc/profile.d/java.sh
source /etc/profile.d/java.sh
```

### Verify

```bash
java -version
# openjdk version "11.0.31" 2026-04-21
```

---

## Step 3: Create Hadoop User and Passwordless SSH

### Create hadoop user (all nodes)

```bash
useradd -m -s /bin/bash hadoop
echo 'hadoop:hadoop123' | chpasswd
usermod -aG sudo hadoop
echo 'hadoop ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/hadoop
chmod 440 /etc/sudoers.d/hadoop
```

### Add hadoop to SSH AllowUsers (all nodes)

Edit `/etc/ssh/sshd_config` and add:

```
PubkeyAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys
AllowUsers hadoop
```

Then restart SSH:

```bash
systemctl restart ssh
```

### Generate SSH key (master only)

```bash
su - hadoop
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
```

### Distribute public key (from master to all nodes including itself)

```bash
# Copy to all nodes
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys  # master itself
# Copy to worker1 and worker2 authorized_keys
```

### SSH Config (`/home/hadoop/.ssh/config`) — all nodes

```
Host master
    HostName 192.168.10.56
    Port 2112
    User hadoop
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR

Host worker1
    HostName 192.168.10.57
    Port 2112
    User hadoop
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR

Host worker2
    HostName 192.168.10.58
    Port 2112
    User hadoop
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
```

### Verify passwordless SSH

```bash
su - hadoop
ssh master hostname   # should return: master
ssh worker1 hostname  # should return: worker1
ssh worker2 hostname  # should return: worker2
```

---

## Step 4: Install Hadoop 3.3.6

### Download and extract (all nodes)

```bash
cd /opt
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 hadoop
chown -R hadoop:hadoop /opt/hadoop
```

### Set environment variables

Add to `/home/hadoop/.bashrc` on all nodes:

```bash
# Hadoop Environment Variables
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
```

---

## Step 5: Configure Hadoop

### Create data directories

```bash
# On master
mkdir -p /opt/hadoop_data/hdfs/namenode
mkdir -p /opt/hadoop_data/hdfs/datanode
mkdir -p /opt/hadoop_data/tmp
chown -R hadoop:hadoop /opt/hadoop_data

# On workers
mkdir -p /opt/hadoop_data/hdfs/datanode
mkdir -p /opt/hadoop_data/tmp
chown -R hadoop:hadoop /opt/hadoop_data
```

### hadoop-env.sh (`/opt/hadoop/etc/hadoop/hadoop-env.sh`)

Add these lines:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Custom SSH port for Hadoop cluster
export HADOOP_SSH_OPTS="-p 2112 -o StrictHostKeyChecking=no"
```

### core-site.xml (`/opt/hadoop/etc/hadoop/core-site.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/hadoop_data/tmp</value>
    </property>
</configuration>
```

### hdfs-site.xml (`/opt/hadoop/etc/hadoop/hdfs-site.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///opt/hadoop_data/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///opt/hadoop_data/hdfs/datanode</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>master:9870</value>
    </property>
</configuration>
```

### yarn-site.xml (`/opt/hadoop/etc/hadoop/yarn-site.xml`)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>master</value>
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
        <name>yarn.nodemanager.resource.cpu-vcores</name>
        <value>2</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
```

### mapred-site.xml (`/opt/hadoop/etc/hadoop/mapred-site.xml`)

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
        <value>HADOOP_MAPRED_HOME=/opt/hadoop</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/opt/hadoop</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/opt/hadoop</value>
    </property>
</configuration>
```

### workers (`/opt/hadoop/etc/hadoop/workers`)

```
worker1
worker2
```

### Distribute configs to workers

```bash
su - hadoop
for host in worker1 worker2; do
  scp $HADOOP_HOME/etc/hadoop/core-site.xml ${host}:$HADOOP_HOME/etc/hadoop/
  scp $HADOOP_HOME/etc/hadoop/hdfs-site.xml ${host}:$HADOOP_HOME/etc/hadoop/
  scp $HADOOP_HOME/etc/hadoop/yarn-site.xml ${host}:$HADOOP_HOME/etc/hadoop/
  scp $HADOOP_HOME/etc/hadoop/mapred-site.xml ${host}:$HADOOP_HOME/etc/hadoop/
  scp $HADOOP_HOME/etc/hadoop/workers ${host}:$HADOOP_HOME/etc/hadoop/
  scp $HADOOP_HOME/etc/hadoop/hadoop-env.sh ${host}:$HADOOP_HOME/etc/hadoop/
done
```

---

## Step 6: Format HDFS and Start Hadoop

### Format NameNode (master only, first time only)

```bash
su - hadoop
hdfs namenode -format
```

### Start HDFS

```bash
start-dfs.sh
```

### Start YARN

```bash
start-yarn.sh
```

### Verify processes

```bash
# On master
jps
# Expected: NameNode, SecondaryNameNode, ResourceManager

# On workers
jps
# Expected: DataNode, NodeManager
```

---

## Step 7: Install Apache Spark 3.5.9

### Download and extract (all nodes)

```bash
cd /opt
wget https://dlcdn.apache.org/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz
tar -xzf spark-3.5.9-bin-hadoop3.tgz
mv spark-3.5.9-bin-hadoop3 spark
chown -R hadoop:hadoop /opt/spark
```

### Add to `/home/hadoop/.bashrc` (all nodes)

```bash
# Spark Environment Variables
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```

---

## Step 8: Configure Spark on YARN

### spark-defaults.conf (`/opt/spark/conf/spark-defaults.conf`)

```properties
spark.master                     yarn
spark.submit.deployMode          client
spark.driver.memory              512m
spark.executor.memory            512m
spark.executor.cores             1
spark.executor.instances         2
spark.yarn.am.memory             512m
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://master:9000/spark-logs
spark.history.fs.logDirectory    hdfs://master:9000/spark-logs
spark.history.provider           org.apache.spark.deploy.history.FsHistoryProvider
```

### spark-env.sh (`/opt/spark/conf/spark-env.sh`)

```bash
#!/usr/bin/env bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export PYSPARK_PYTHON=python3
```

### Create Spark log directory in HDFS

```bash
su - hadoop
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -chmod 777 /spark-logs
```

### Distribute Spark configs to workers

```bash
for host in worker1 worker2; do
  scp /opt/spark/conf/spark-defaults.conf ${host}:/opt/spark/conf/
  scp /opt/spark/conf/spark-env.sh ${host}:/opt/spark/conf/
done
```

---

## Step 9: Verify Cluster

### Test HDFS

```bash
echo "Hello Hadoop!" > /tmp/test.txt
hdfs dfs -mkdir -p /user/hadoop
hdfs dfs -put /tmp/test.txt /user/hadoop/
hdfs dfs -cat /user/hadoop/test.txt
hdfs dfsadmin -report
```

### Test YARN (MapReduce)

```bash
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar pi 2 5
```

### Test Spark on YARN (cluster mode)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 512m \
  --executor-memory 512m \
  --num-executors 2 \
  --class org.apache.spark.examples.SparkPi \
  $SPARK_HOME/examples/jars/spark-examples_2.12-3.5.9.jar 10
```

### Test PySpark on YARN (client mode)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  $SPARK_HOME/examples/src/main/python/pi.py 10
```

---

## Web UIs

| Service | URL | Description |
|---------|-----|-------------|
| HDFS NameNode | http://192.168.10.56:9870 | HDFS file system browser & status |
| YARN ResourceManager | http://192.168.10.56:8088 | Job tracking & node status |
| Spark History Server | http://192.168.10.56:18080 | Spark job history (after starting) |

### Start Spark History Server (optional)

```bash
$SPARK_HOME/sbin/start-history-server.sh
```

---

## Cluster Management Commands

### Start cluster

```bash
su - hadoop
start-dfs.sh        # Start HDFS (NameNode + DataNodes)
start-yarn.sh       # Start YARN (ResourceManager + NodeManagers)
```

### Stop cluster

```bash
su - hadoop
stop-yarn.sh        # Stop YARN
stop-dfs.sh         # Stop HDFS
```

### Check cluster status

```bash
hdfs dfsadmin -report          # HDFS health
yarn node -list                # YARN node status
yarn application -list         # Running applications
```

---

## Directory Structure

```
/opt/hadoop/                    # Hadoop installation
/opt/hadoop/etc/hadoop/         # Hadoop config files
/opt/hadoop_data/hdfs/namenode/ # NameNode data (master only)
/opt/hadoop_data/hdfs/datanode/ # DataNode data (workers)
/opt/hadoop_data/tmp/           # Hadoop temp directory
/opt/spark/                     # Spark installation
/opt/spark/conf/                # Spark config files
/home/hadoop/                   # Hadoop user home
/home/hadoop/.ssh/              # SSH keys and config
```

---

## Ports Reference

| Port | Service |
|------|---------|
| 2112 | SSH (custom) |
| 9000 | HDFS NameNode RPC |
| 9870 | HDFS NameNode Web UI |
| 8088 | YARN ResourceManager Web UI |
| 8042 | YARN NodeManager Web UI |
| 7077 | Spark Master (standalone mode) |
| 8080 | Spark Master Web UI (standalone) |
| 18080 | Spark History Server |

---

## Troubleshooting

### Services not starting
```bash
# Check logs
cat /opt/hadoop/logs/hadoop-hadoop-namenode-master.log
cat /opt/hadoop/logs/hadoop-hadoop-datanode-worker1.log
```

### HDFS in safe mode
```bash
hdfs dfsadmin -safemode leave
```

### DataNode not connecting
- Verify /etc/hosts is correct on all nodes
- Check firewall: `ufw status` (disable for lab: `ufw disable`)
- Verify passwordless SSH works between nodes

### Spark job failing on YARN
- Check YARN logs: `yarn logs -applicationId <app_id>`
- Ensure executor memory doesn't exceed `yarn.nodemanager.resource.memory-mb`
- Verify HADOOP_CONF_DIR and YARN_CONF_DIR are set

---

## Notes

- **Replication factor:** Set to 2 (suitable for 2 DataNodes)
- **Memory allocation:** Conservative (512m) for 4 GB RAM nodes
- **SSH port:** Custom port 2112 (configured in hadoop-env.sh via HADOOP_SSH_OPTS)
- **HDFS Capacity:** ~58 GB total across 2 DataNodes
- This setup is for **lab/learning purposes** only, not production
