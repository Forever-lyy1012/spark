# 步骤4b：实时流处理 - Spark Structured Streaming

import os
import findspark
findspark.init()
from pyspark.sql import SparkSession, functions as F
import time

CHECKPOINT_DIR = "D:/Spark/checkpoint/"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

USE_KAFKA = True           # Kafka 模式: True, Socket 模式: False
KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'video_events'
SOCKET_HOST = 'localhost'
SOCKET_PORT = 9998

spark = SparkSession.builder \
    .appName("StreamingAnalysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ===== 读取数据源并解析字段 =====
if USE_KAFKA:
    print(f"数据源: Kafka ({KAFKA_BOOTSTRAP}, Topic: {KAFKA_TOPIC})")
    stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest").load() \
        .selectExpr("CAST(value AS STRING) as line") \
        .select(
            F.split("line", ",").getItem(0).alias("UserID"),
            F.split("line", ",").getItem(1).alias("VideoID"),
            F.split("line", ",").getItem(2).alias("Action"),
            F.split("line", ",").getItem(3).cast("int").alias("WatchTime"),
            F.split("line", ",").getItem(4).alias("EventTime"),
        ) \
        .withColumn("ts", F.current_timestamp())
else:
    print(f"数据源: Socket ({SOCKET_HOST}:{SOCKET_PORT})")
    stream = spark.readStream.format("socket") \
        .option("host", SOCKET_HOST).option("port", SOCKET_PORT).load() \
        .select(
            F.split("value", ",").getItem(0).alias("UserID"),
            F.split("value", ",").getItem(1).alias("VideoID"),
            F.split("value", ",").getItem(2).alias("Action"),
            F.split("value", ",").getItem(3).cast("int").alias("WatchTime"),
            F.split("value", ",").getItem(4).alias("EventTime"),
        ) \
        .withColumn("ts", F.current_timestamp())

# ===== 查询1: 实时热门视频 Top10 =====
print("查询1: 实时热门视频")
hot = stream.groupBy(F.window("ts", "15 seconds", "5 seconds"), F.col("VideoID")) \
    .agg(
        F.count(F.when(F.col("Action") == "play", 1)).alias("plays"),
        F.count(F.when(F.col("Action") == "like", 1)).alias("likes"),
        F.count("*").alias("total"),
    ) \
    .withColumn("hot", F.col("plays") + F.col("likes") * 3)

q1 = hot.writeStream.outputMode("complete").format("memory") \
    .queryName("hot_videos") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", CHECKPOINT_DIR + "hot").start()

# ===== 查询2: 每10秒行为统计 =====
print("查询2: 行为统计")
q2 = stream.groupBy(F.window("ts", "15 seconds", "5 seconds"), F.col("Action")).count() \
    .writeStream.outputMode("complete").format("memory") \
    .queryName("action_stats") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", CHECKPOINT_DIR + "action").start()

# ===== 查询3: 控制台实时输出 =====
print("查询3: 控制台输出")
q3 = stream.groupBy(F.window("ts", "15 seconds", "5 seconds"), F.col("Action")).count() \
    .writeStream.outputMode("complete").format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", CHECKPOINT_DIR + "console").start()

print("\n流处理已启动，每10秒输出一次")
print("按 Ctrl+C 停止\n")

try:
    while True:
        time.sleep(12)

        # 热门视频
        try:
            df = spark.sql("""
                SELECT window, VideoID, hot, plays, likes
                FROM hot_videos ORDER BY hot DESC LIMIT 5
            """)
            if df.count() > 0:
                print(f"\n[{time.strftime('%H:%M:%S')}] 实时热门 Top5:")
                df.show(5, truncate=False)
        except:
            pass

        # 行为统计
        try:
            df = spark.sql("""
                SELECT window, Action, count FROM action_stats
                ORDER BY window DESC, count DESC LIMIT 10
            """)
            if df.count() > 0:
                print(f"行为分布:")
                df.show(10, truncate=False)
        except:
            pass

except KeyboardInterrupt:
    pass
finally:
    for q in [q1, q2, q3]:
        q.stop()
    spark.stop()
    print("已停止")
