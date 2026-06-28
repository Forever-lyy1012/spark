# 步骤2：离线分析 - RDD统计 + Spark SQL多维分析

import os
import findspark
findspark.init()
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

DATA_DIR = "D:/Spark/data/"
OUTPUT_DIR = "D:/Spark/output/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("OfflineAnalysis") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ===== 1. 读取数据 =====
print("=" * 50)
print("1. 读取数据")

behavior_schema = StructType([
    StructField("UserID", StringType()),
    StructField("VideoID", StringType()),
    StructField("Action", StringType()),
    StructField("WatchTime", IntegerType()),
    StructField("EventTime", StringType()),
])

video_schema = StructType([
    StructField("VideoID", StringType()),
    StructField("Title", StringType()),
    StructField("Category", StringType()),
    StructField("Duration_sec", IntegerType()),
    StructField("UploadDate", StringType()),
    StructField("UploaderID", StringType()),
])

behavior_df = spark.read.csv(DATA_DIR + "video_behavior_history.csv", header=True, schema=behavior_schema)
video_df = spark.read.csv(DATA_DIR + "video_info.csv", header=True, schema=video_schema)

behavior_df.createOrReplaceTempView("behavior")
video_df.createOrReplaceTempView("video")

print(f"行为数据: {behavior_df.count()} 条")
print(f"视频数据: {video_df.count()} 条")

# ===== 2. 数据清洗 =====
print("\n" + "=" * 50)
print("2. 数据清洗")

# 检查缺失值
for col in behavior_df.columns:
    cnt = behavior_df.filter(F.col(col).isNull()).count()
    if cnt > 0:
        print(f"  {col}: {cnt} 个缺失")

# 清洗
valid_actions = ['play', 'like', 'comment', 'share', 'favorite', 'follow']
behavior_clean = behavior_df \
    .filter(F.col("WatchTime") >= 0) \
    .filter(F.col("Action").isin(valid_actions)) \
    .filter(F.col("EventTime").isNotNull()) \
    .filter(F.col("UserID").isNotNull()) \
    .filter(F.col("VideoID").isNotNull())

behavior_clean.createOrReplaceTempView("behavior_clean")

# 解析时间
behavior_clean = behavior_clean.withColumn("EventHour",
    F.hour(F.to_timestamp("EventTime", "yyyy-MM-dd HH:mm:ss")))
behavior_clean = behavior_clean.withColumn("EventDate",
    F.to_date(F.to_timestamp("EventTime", "yyyy-MM-dd HH:mm:ss")))
behavior_clean.createOrReplaceTempView("behavior_clean")

removed = behavior_df.count() - behavior_clean.count()
print(f"清洗后: {behavior_clean.count()} 条, 移除: {removed} 条")

# ===== 3. RDD 统计分析 =====
print("\n" + "=" * 50)
print("3. RDD 统计")

# 3.1 用户活跃度排名
print("\n--- 用户行为次数 Top10 ---")
user_rdd = behavior_clean.select("UserID").rdd \
    .map(lambda r: (r[0], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)
for i, (uid, cnt) in enumerate(user_rdd.take(10)):
    print(f"  {i+1}. {uid}: {cnt}")

# 3.2 行为类型分布
print("\n--- 行为类型统计 ---")
action_rdd = behavior_clean.select("Action").rdd \
    .map(lambda r: (r[0], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)
for action, cnt in action_rdd.collect():
    print(f"  {action}: {cnt}")

# 3.3 视频播放量 Top10
print("\n--- 视频播放量 Top10 ---")
play_rdd = behavior_clean.filter(F.col("Action") == "play").select("VideoID").rdd \
    .map(lambda r: (r[0], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)
for i, (vid, cnt) in enumerate(play_rdd.take(10)):
    print(f"  {i+1}. {vid}: {cnt} 次")

# 3.4 总观看时长 Top10
print("\n--- 总观看时长 Top10 ---")
watch_rdd = behavior_clean.filter(F.col("Action") == "play").select("VideoID", "WatchTime").rdd \
    .map(lambda r: (r[0], r[1])) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)
for i, (vid, wt) in enumerate(watch_rdd.take(10)):
    print(f"  {i+1}. {vid}: {wt} 秒")

# ===== 4. Spark SQL 分析 =====
print("\n" + "=" * 50)
print("4. Spark SQL 多维分析")

# 4.1 综合热度排行
print("\n--- 综合热度 Top10 ---")
hot_df = spark.sql("""
    SELECT v.VideoID, v.Title, v.Category,
           COUNT(CASE WHEN b.Action='play' THEN 1 END) AS plays,
           COUNT(CASE WHEN b.Action='like' THEN 1 END) AS likes,
           COUNT(CASE WHEN b.Action='comment' THEN 1 END) AS comments,
           COUNT(CASE WHEN b.Action='share' THEN 1 END) AS shares,
           COUNT(CASE WHEN b.Action='favorite' THEN 1 END) AS favorites
    FROM behavior_clean b JOIN video v ON b.VideoID = v.VideoID
    GROUP BY v.VideoID, v.Title, v.Category
    ORDER BY plays*1 + likes*3 + comments*5 + shares*8 + favorites*5 DESC
    LIMIT 10
""")
hot_df.show(10, truncate=False)

# 4.2 类别分析
print("\n--- 各类别统计 ---")
cat_df = spark.sql("""
    SELECT v.Category,
           COUNT(*) AS total,
           COUNT(DISTINCT v.VideoID) AS videos,
           COUNT(DISTINCT b.UserID) AS users,
           COUNT(CASE WHEN b.Action='play' THEN 1 END) AS plays,
           COUNT(CASE WHEN b.Action='like' THEN 1 END) AS likes,
           ROUND(COUNT(CASE WHEN b.Action='like' THEN 1 END)*100.0/
                 NULLIF(COUNT(CASE WHEN b.Action='play' THEN 1 END),0), 2) AS like_rate
    FROM behavior_clean b JOIN video v ON b.VideoID = v.VideoID
    GROUP BY v.Category
    ORDER BY total DESC
""")
cat_df.show(10, truncate=False)

# 4.3 时段分析
print("\n--- 24小时活跃度 ---")
hour_df = spark.sql("""
    SELECT EventHour, COUNT(*) AS cnt, COUNT(DISTINCT UserID) AS users
    FROM behavior_clean
    GROUP BY EventHour
    ORDER BY EventHour
""")
hour_df.show(24, truncate=False)

# 4.4 互动率
print("\n--- 整体互动率 ---")
rate_df = spark.sql("""
    SELECT
        COUNT(DISTINCT UserID) AS total_users,
        COUNT(DISTINCT VideoID) AS total_videos,
        SUM(CASE WHEN Action='play' THEN 1 ELSE 0 END) AS total_plays,
        SUM(CASE WHEN Action='like' THEN 1 ELSE 0 END) AS total_likes,
        ROUND(SUM(CASE WHEN Action='like' THEN 1 ELSE 0 END)*100.0/
              NULLIF(SUM(CASE WHEN Action='play' THEN 1 ELSE 0 END),0), 2) AS like_rate,
        ROUND(SUM(CASE WHEN Action='comment' THEN 1 ELSE 0 END)*100.0/
              NULLIF(SUM(CASE WHEN Action='play' THEN 1 ELSE 0 END),0), 2) AS comment_rate,
        ROUND(SUM(CASE WHEN Action='share' THEN 1 ELSE 0 END)*100.0/
              NULLIF(SUM(CASE WHEN Action='play' THEN 1 ELSE 0 END),0), 2) AS share_rate
    FROM behavior_clean
""")
rate_df.show(truncate=False)

# 4.5 高互动视频（点赞率最高的视频，至少10次播放）
print("\n--- 高互动率视频 Top10 ---")
hi_df = spark.sql("""
    SELECT v.VideoID, v.Title, v.Category,
           COUNT(CASE WHEN b.Action='play' THEN 1 END) AS plays,
           COUNT(CASE WHEN b.Action='like' THEN 1 END) AS likes,
           ROUND(COUNT(CASE WHEN b.Action='like' THEN 1 END)*100.0/
                 NULLIF(COUNT(CASE WHEN b.Action='play' THEN 1 END),0), 2) AS rate
    FROM behavior_clean b JOIN video v ON b.VideoID = v.VideoID
    GROUP BY v.VideoID, v.Title, v.Category
    HAVING COUNT(CASE WHEN b.Action='play' THEN 1 END) >= 10
    ORDER BY rate DESC
    LIMIT 10
""")
hi_df.show(10, truncate=False)

# ===== 5. 导出结果 =====
print("\n" + "=" * 50)
print("5. 导出结果")

hot_df.toPandas().to_csv(OUTPUT_DIR + "hot_top10.csv", index=False, encoding='utf-8-sig')
cat_df.toPandas().to_csv(OUTPUT_DIR + "category_stats.csv", index=False, encoding='utf-8-sig')
hour_df.toPandas().to_csv(OUTPUT_DIR + "hour_stats.csv", index=False, encoding='utf-8-sig')
rate_df.toPandas().to_csv(OUTPUT_DIR + "interaction_rate.csv", index=False, encoding='utf-8-sig')
print("已导出到 " + OUTPUT_DIR)

spark.stop()
print("离线分析完成")
