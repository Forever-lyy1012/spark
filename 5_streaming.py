# 步骤4b：实时流处理 - Spark Structured Streaming
# 从 Kafka 读取实时事件，窗口聚合 + 实时热门排行

import os
import findspark
findspark.init()
from pyspark.sql import SparkSession, functions as F
import time

CHECKPOINT_DIR = "D:/Spark/checkpoint/"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'video_events'

spark = SparkSession.builder \
    .appName("StreamingAnalysis") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ===== 从 Kafka 读取并解析字段 =====
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

# ===== 查询1: 实时热门视频（15秒窗口，5秒滑动） =====
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

# ===== 查询2: 行为分布统计（写内存，poll 时用 SQL 查） =====
print("查询2: 行为分布")
q2 = stream.groupBy(F.window("ts", "15 seconds", "5 seconds"), F.col("Action")).count() \
    .writeStream.outputMode("complete").format("memory") \
    .queryName("action_stats") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", CHECKPOINT_DIR + "action").start()

print("\n流处理已启动，每12秒输出一次")
print("按 Ctrl+C 停止\n")

try:
    while True:
        time.sleep(12)

        # 实时热门 Top5
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

        # 行为分布
        try:
            df = spark.sql("""
                SELECT window, Action, count FROM action_stats
                ORDER BY window DESC, count DESC LIMIT 10
            """)
            if df.count() > 0:
                print("行为分布:")
                df.show(10, truncate=False)
        except:
            pass

except KeyboardInterrupt:
    pass
finally:
    for q in [q1, q2]:
        q.stop()
    spark.stop()
    print("已停止")
