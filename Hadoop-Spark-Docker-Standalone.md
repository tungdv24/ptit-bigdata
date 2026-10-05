# Hadoop + Spark Standalone with Docker

> Single-node (pseudo-distributed) setup using Docker containers for lab/development purposes.
> No multi-server cluster needed — everything runs on one machine.

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    Docker Host (your machine)             │
│                                                          │
│  ┌─────────────────────────────────────────────────────┐ │
│  │           docker-compose cluster                    │ │
│  │                                                     │ │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐      │ │
│  │  │  namenode │  │ datanode1 │  │ datanode2 │      │ │
│  │  │  (HDFS)   │  │  (HDFS)   │  │  (HDFS)   │      │ │
│  │  └───────────┘  └───────────┘  └───────────┘      │ │
│  │                                                     │ │
│  │  ┌────────────────┐  ┌──────────────────────────┐  │ │
│  │  │ resourcemanager│  │     spark-master          │  │ │
│  │  │    (YARN)      │  │  + history-server         │  │ │
│  │  └────────────────┘  └──────────────────────────┘  │ │
│  │                                                     │ │
│  │  ┌───────────────┐  ┌───────────────┐             │ │
│  │  │  nodemanager1 │  │  nodemanager2 │             │ │
│  │  │ + spark-worker│  │ + spark-worker│             │ │
│  │  └───────────────┘  └───────────────┘             │ │
│  └─────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- Docker Engine 20.10+
- Docker Compose v2+
- At least 8 GB RAM available for Docker
- 10 GB free disk space

### Check Docker is installed

```bash
docker --version
docker compose version
```

---

## Project Structure

```
hadoop-spark-docker/
├── docker-compose.yml
├── Dockerfile
├── config/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── yarn-site.xml
│   ├── mapred-site.xml
│   ├── workers
│   ├── hadoop-env.sh
│   ├── spark-defaults.conf
│   └── spark-env.sh
├── scripts/
│   ├── start-hadoop.sh
│   ├── start-spark.sh
│   └── bootstrap.sh
└── data/
    └── (your data files)
```

---

## Step 1: Create Project Directory

```bash
mkdir -p hadoop-spark-docker/{config,scripts,data}
cd hadoop-spark-docker
```

---

## Step 2: Dockerfile

Create `Dockerfile`:

```dockerfile
FROM ubuntu:22.04

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    ssh \
    rsync \
    curl \
    wget \
    python3 \
    python3-pip \
    net-tools \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# ============ HADOOP ============
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN wget -q https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# ============ SPARK ============
ENV SPARK_VERSION=3.5.9
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN wget -q https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# ============ SSH Setup ============
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 600 ~/.ssh/authorized_keys

RUN echo "Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR" > ~/.ssh/config

# Create data directories
RUN mkdir -p /opt/hadoop_data/hdfs/namenode \
    && mkdir -p /opt/hadoop_data/hdfs/datanode \
    && mkdir -p /opt/hadoop_data/tmp \
    && mkdir -p /opt/spark/logs

# SSH config for service
RUN echo "Port 22" >> /etc/ssh/sshd_config \
    && echo "PermitRootLogin yes" >> /etc/ssh/sshd_config \
    && echo "PubkeyAuthentication yes" >> /etc/ssh/sshd_config

# Environment
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
ENV HADOOP_MAPRED_HOME=$HADOOP_HOME
ENV HADOOP_COMMON_HOME=$HADOOP_HOME
ENV HADOOP_HDFS_HOME=$HADOOP_HOME
ENV HADOOP_YARN_HOME=$HADOOP_HOME

# Copy configs
COPY config/core-site.xml $HADOOP_CONF_DIR/core-site.xml
COPY config/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
COPY config/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml
COPY config/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
COPY config/workers $HADOOP_CONF_DIR/workers
COPY config/hadoop-env.sh $HADOOP_CONF_DIR/hadoop-env.sh
COPY config/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
COPY config/spark-env.sh $SPARK_HOME/conf/spark-env.sh

# Copy scripts
COPY scripts/ /scripts/
RUN chmod +x /scripts/*.sh

EXPOSE 9870 8088 9000 7077 8080 18080 4040 8042

CMD ["/scripts/bootstrap.sh"]
```

---

## Step 3: Configuration Files

### config/core-site.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/hadoop_data/tmp</value>
    </property>
</configuration>
```

### config/hdfs-site.xml

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
        <value>namenode:9870</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
</configuration>
```

### config/yarn-site.xml

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
        <value>resourcemanager</value>
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
        <name>yarn.nodemanager.pmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
```

### config/mapred-site.xml

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

### config/workers

```
datanode1
datanode2
```

### config/hadoop-env.sh

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
```

### config/spark-defaults.conf

```properties
spark.master                     yarn
spark.submit.deployMode          client
spark.driver.memory              512m
spark.executor.memory            512m
spark.executor.cores             1
spark.executor.instances         2
spark.yarn.am.memory             512m
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs://namenode:9000/spark-logs
spark.history.fs.logDirectory    hdfs://namenode:9000/spark-logs
spark.history.provider           org.apache.spark.deploy.history.FsHistoryProvider
```

### config/spark-env.sh

```bash
#!/usr/bin/env bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export PYSPARK_PYTHON=python3
export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://namenode:9000/spark-logs"
```

---

## Step 4: Bootstrap Scripts

### scripts/bootstrap.sh

```bash
#!/bin/bash

# Start SSH daemon
service ssh start

# Keep container running
tail -f /dev/null
```

### scripts/start-hadoop.sh

```bash
#!/bin/bash
echo "=== Starting Hadoop Cluster ==="

# Format namenode if not already formatted
if [ ! -d "/opt/hadoop_data/hdfs/namenode/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format -force
fi

echo "Starting HDFS..."
start-dfs.sh

echo "Starting YARN..."
start-yarn.sh

# Create Spark logs directory in HDFS
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -chmod 777 /spark-logs
hdfs dfs -mkdir -p /user/root

echo "=== Hadoop Started ==="
hdfs dfsadmin -report | head -10
echo ""
yarn node -list 2>/dev/null | grep -v INFO
```

### scripts/start-spark.sh

```bash
#!/bin/bash
echo "=== Starting Spark History Server ==="

$SPARK_HOME/sbin/start-history-server.sh

echo "Spark History Server: http://localhost:18080"
echo ""
echo "To submit a Spark job:"
echo "  spark-submit --master yarn --deploy-mode client your_app.py"
echo ""
echo "To test:"
echo "  spark-submit --master yarn --deploy-mode client \$SPARK_HOME/examples/src/main/python/pi.py 10"
```

---

## Step 5: Docker Compose

### docker-compose.yml

```yaml
version: '3.8'

services:
  namenode:
    build: .
    container_name: namenode
    hostname: namenode
    networks:
      - hadoop-net
    ports:
      - "9870:9870"   # HDFS Web UI
      - "9000:9000"   # HDFS RPC
    volumes:
      - namenode_data:/opt/hadoop_data/hdfs/namenode
      - ./data:/data
    environment:
      - CLUSTER_NAME=hadoop-lab
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9870"]
      interval: 30s
      timeout: 10s
      retries: 3

  datanode1:
    build: .
    container_name: datanode1
    hostname: datanode1
    networks:
      - hadoop-net
    volumes:
      - datanode1_data:/opt/hadoop_data/hdfs/datanode
    depends_on:
      - namenode

  datanode2:
    build: .
    container_name: datanode2
    hostname: datanode2
    networks:
      - hadoop-net
    volumes:
      - datanode2_data:/opt/hadoop_data/hdfs/datanode
    depends_on:
      - namenode

  resourcemanager:
    build: .
    container_name: resourcemanager
    hostname: resourcemanager
    networks:
      - hadoop-net
    ports:
      - "8088:8088"   # YARN Web UI
    depends_on:
      - namenode
      - datanode1
      - datanode2

  nodemanager1:
    build: .
    container_name: nodemanager1
    hostname: nodemanager1
    networks:
      - hadoop-net
    depends_on:
      - resourcemanager

  nodemanager2:
    build: .
    container_name: nodemanager2
    hostname: nodemanager2
    networks:
      - hadoop-net
    depends_on:
      - resourcemanager

  spark-master:
    build: .
    container_name: spark-master
    hostname: spark-master
    networks:
      - hadoop-net
    ports:
      - "8080:8080"   # Spark Master UI
      - "7077:7077"   # Spark Master RPC
      - "18080:18080" # Spark History Server
      - "4040:4040"   # Spark Application UI
    volumes:
      - ./data:/data
    depends_on:
      - resourcemanager
      - nodemanager1
      - nodemanager2

networks:
  hadoop-net:
    driver: bridge

volumes:
  namenode_data:
  datanode1_data:
  datanode2_data:
```

---

## Step 6: Build and Run

### Build the Docker image

```bash
cd hadoop-spark-docker
docker compose build
```

### Start all containers

```bash
docker compose up -d
```

### Wait for containers to start, then initialize the cluster

```bash
# Wait ~10 seconds for all containers to be ready
sleep 10

# Format and start Hadoop (run from namenode)
docker exec -it namenode bash -c "/scripts/start-hadoop.sh"

# Start Spark History Server
docker exec -it spark-master bash -c "/scripts/start-spark.sh"
```

---

## Step 7: Verify

### Check HDFS

```bash
docker exec -it namenode hdfs dfsadmin -report
```

### Check YARN nodes

```bash
docker exec -it resourcemanager yarn node -list
```

### Run Spark Pi test

```bash
docker exec -it spark-master spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --num-executors 2 \
  $SPARK_HOME/examples/src/main/python/pi.py 10
```

### Run PySpark interactive shell

```bash
docker exec -it spark-master pyspark --master yarn
```

---

## Web UIs

| Service | URL | Description |
|---------|-----|-------------|
| HDFS NameNode | http://localhost:9870 | Browse HDFS, check DataNode health |
| YARN ResourceManager | http://localhost:8088 | Monitor jobs, node status |
| Spark History Server | http://localhost:18080 | Completed Spark job details |
| Spark Application UI | http://localhost:4040 | Running Spark job details |

---

## Usage Examples

### Upload data to HDFS

```bash
# Copy a file into the namenode container
docker cp myfile.csv namenode:/data/

# Upload to HDFS from inside the container
docker exec -it namenode hdfs dfs -put /data/myfile.csv /user/root/
```

### Submit a PySpark job

```bash
# Copy your script into the spark-master container
docker cp my_analysis.py spark-master:/data/

# Run it on YARN
docker exec -it spark-master spark-submit \
  --master yarn \
  --deploy-mode client \
  /data/my_analysis.py
```

### Interactive Spark SQL

```bash
docker exec -it spark-master spark-sql --master yarn
```

```sql
-- Inside spark-sql shell
CREATE TEMPORARY VIEW logs USING csv OPTIONS (path 'hdfs://namenode:9000/user/root/access.csv', header 'true');
SELECT * FROM logs WHERE status = 500 LIMIT 10;
```

---

## Cluster Management

### Stop cluster

```bash
docker compose down
```

### Stop cluster but keep data

```bash
docker compose stop
```

### Restart cluster

```bash
docker compose start
sleep 10
docker exec -it namenode bash -c "/scripts/start-hadoop.sh"
docker exec -it spark-master bash -c "/scripts/start-spark.sh"
```

### Remove everything (including data volumes)

```bash
docker compose down -v
```

### View logs

```bash
docker logs namenode
docker logs resourcemanager
docker logs spark-master
```

### Scale datanodes (add more storage)

```bash
docker compose up -d --scale datanode1=1 --scale datanode2=1
```

---

## Comparison: Docker vs Bare-Metal Cluster

| Aspect | Docker (this guide) | Bare-Metal (3 servers) |
|--------|--------------------|-----------------------|
| Setup time | ~5 minutes | ~30 minutes |
| Servers needed | 1 machine | 3 servers |
| Performance | Lower (shared resources) | Full hardware |
| Portability | Move anywhere with docker | Tied to specific servers |
| Reset/rebuild | `docker compose down -v && up` | Manual reinstall |
| Best for | Development, learning, testing | Production-like lab, benchmarks |

---

## Troubleshooting

### Container won't start
```bash
docker compose logs <container_name>
```

### HDFS in safe mode
```bash
docker exec -it namenode hdfs dfsadmin -safemode leave
```

### DataNode not connecting to NameNode
```bash
# Check network connectivity
docker exec -it datanode1 ping namenode

# Check DataNode logs
docker logs datanode1
```

### Spark job OOM (Out of Memory)
- Reduce executor memory in spark-defaults.conf
- Or increase Docker memory limit in Docker Desktop settings

### Port conflicts
If ports 9870, 8088, etc. are already in use, change the left-side port in docker-compose.yml:
```yaml
ports:
  - "19870:9870"  # Maps host port 19870 to container port 9870
```

---

## Quick Start (TL;DR)

```bash
# Clone/create the project
mkdir hadoop-spark-docker && cd hadoop-spark-docker

# (create all files as described above, or clone from repo)

# Build and start
docker compose build
docker compose up -d
sleep 15

# Initialize
docker exec -it namenode bash -c "/scripts/start-hadoop.sh"
docker exec -it spark-master bash -c "/scripts/start-spark.sh"

# Test
docker exec -it spark-master spark-submit --master yarn \
  $SPARK_HOME/examples/src/main/python/pi.py 10

# Access UIs
# HDFS: http://localhost:9870
# YARN: http://localhost:8088
# Spark: http://localhost:18080
```
