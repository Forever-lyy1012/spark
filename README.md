# 课题15：短视频用户行为分析与内容推荐系统

Spark 大数据处理课程设计，技术路线：RDD + Spark SQL + Structured Streaming + ALS 推荐 + 可视化。

## 环境要求

| 组件 | 说明 |
|------|------|
| Python | 3.10+ |
| JDK | 17+（推荐 Eclipse Temurin） |
| Kafka | 3.9+（实时处理需要） |

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 安装 JDK 17

下载 Eclipse Temurin JDK 17：https://adoptium.net/download/
解压到项目目录 `jdk17/`，确保 `jdk17/jdk-17.0.xx+xx/bin/java.exe` 存在。

### 3. 安装 Kafka（实时处理需要）

```bash
# 下载
curl -L -o kafka.tgz "https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/3.9.2/kafka_2.13-3.9.2.tgz"
tar -xzf kafka.tgz

# 修改 Zookeeper 端口（避免端口冲突）
sed -i 's|dataDir=/tmp/zookeeper|dataDir=D:/Spark/kafka_data/zk|' kafka_2.13-3.9.2/config/zookeeper.properties
sed -i 's|clientPort=2181|clientPort=12181|' kafka_2.13-3.9.2/config/zookeeper.properties

# 修改 Kafka 配置
sed -i 's|log.dirs=/tmp/kafka-logs|log.dirs=D:/Spark/kafka_data/kafka|' kafka_2.13-3.9.2/config/server.properties
sed -i 's|zookeeper.connect=localhost:2181|zookeeper.connect=localhost:12181|' kafka_2.13-3.9.2/config/server.properties
```

### 4. 设置环境变量

```bash
export JAVA_HOME="D:/Spark/jdk17/jdk-17.0.19+10"
export PATH="$JAVA_HOME/bin:$PATH"
```

## 运行

### 离线分析（一键）

```bash
python run_all.py
```

### 实时处理（三个终端）

```bash
# 终端1：Zookeeper
cd kafka_2.13-3.9.2
bin/zookeeper-server-start.sh config/zookeeper.properties

# 终端2：Kafka
cd kafka_2.13-3.9.2
bin/kafka-server-start.sh config/server.properties

# 终端3a：生产者
python 4_kafka_producer.py

# 终端3b：流处理
python 5_streaming.py
```

## 项目结构

| 文件 | 说明 |
|------|------|
| 1_generate_data.py | 生成模拟数据 |
| 2_offline_analysis.py | RDD + Spark SQL 离线分析 |
| 3_recommendation.py | ALS 推荐算法 |
| 4_kafka_producer.py | Kafka 实时事件生产者 |
| 5_streaming.py | Structured Streaming 流处理 |
| 6_visualization.py | 数据可视化 |
| run_all.py | 一键运行 |
| 项目说明.md | 详细文档 |

详细说明见 [项目说明.md](项目说明.md)。
