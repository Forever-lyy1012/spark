# 步骤3：推荐算法 - 热度推荐 + ALS协同过滤 + 用户画像

import os
import findspark
findspark.init()
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.sql.window import Window

DATA_DIR = "D:/Spark/data/"
OUTPUT_DIR = "D:/Spark/output/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("Recommendation") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ===== 1. 加载数据 =====
print("=" * 50)
print("1. 加载数据")

behavior_df = spark.read.csv(DATA_DIR + "video_behavior_history.csv", header=True, inferSchema=True)
video_df = spark.read.csv(DATA_DIR + "video_info.csv", header=True, inferSchema=True)
print(f"行为数据: {behavior_df.count()} 条, 视频: {video_df.count()} 个")

# ===== 2. 构建评分矩阵 =====
print("\n" + "=" * 50)
print("2. 构建用户-视频评分矩阵")

# 行为 -> 评分: play=1, like=3, comment=2, share=4, favorite=5, follow=2
score_map = {
    'play': 1.0, 'like': 3.0, 'comment': 2.0,
    'share': 4.0, 'favorite': 5.0, 'follow': 2.0
}

score_expr = F.when(F.col("Action") == "play", 1.0)
for a, s in score_map.items():
    if a != 'play':
        score_expr = score_expr.when(F.col("Action") == a, s)

rating_df = behavior_df.withColumn("score", score_expr) \
    .groupBy("UserID", "VideoID") \
    .agg(F.sum("score").alias("rating"), F.count("*").alias("cnt")) \
    .withColumn("rating", F.when(F.col("rating") > 10, 10.0).otherwise(F.col("rating")))

print(f"评分记录数: {rating_df.count()}")

# ===== 3. 编码 + ALS 训练 =====
print("\n" + "=" * 50)
print("3. ALS 协同过滤训练")

# ID编码
user_idx = StringIndexer(inputCol="UserID", outputCol="user_idx", handleInvalid="skip")
item_idx = StringIndexer(inputCol="VideoID", outputCol="item_idx", handleInvalid="skip")

rating_enc = user_idx.fit(rating_df).transform(rating_df)
rating_enc = item_idx.fit(rating_enc).transform(rating_enc)
rating_enc = rating_enc.withColumn("user_idx", F.col("user_idx").cast(IntegerType()))
rating_enc = rating_enc.withColumn("item_idx", F.col("item_idx").cast(IntegerType()))
rating_enc = rating_enc.withColumn("rating", F.col("rating").cast(FloatType()))

num_models = int(rating_enc.agg(F.max("user_idx")).collect()[0][0]) + 1
num_items = int(rating_enc.agg(F.max("item_idx")).collect()[0][0]) + 1
print(f"用户数: {num_models}, 视频数: {num_items}")

# 划分训练/测试
train, test = rating_enc.randomSplit([0.8, 0.2], seed=42)

als = ALS(
    maxIter=10, regParam=0.01, rank=20,
    userCol="user_idx", itemCol="item_idx", ratingCol="rating",
    coldStartStrategy="drop", nonnegative=True
)
model = als.fit(train)

# 评估
preds = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(preds)
print(f"RMSE = {rmse:.4f}")

# ===== 4. 生成推荐 =====
print("\n" + "=" * 50)
print("4. 生成 Top10 个性化推荐")

# 取50个用户做推荐演示
users_subset = rating_enc.select("user_idx").distinct().limit(50)
recs = model.recommendForUserSubset(users_subset, 10)
recs.show(5, truncate=False)

# ===== 5. 热度推荐（Baseline对比） =====
print("\n" + "=" * 50)
print("5. 热度推荐（对比基准）")

hot_recs = behavior_df \
    .withColumn("s", score_expr) \
    .groupBy("VideoID").agg(F.sum("s").alias("hot_score"), F.count("*").alias("cnt")) \
    .join(video_df, "VideoID") \
    .select("VideoID", "Title", "Category", "hot_score") \
    .orderBy(F.desc("hot_score")).limit(10)
hot_recs.show(10, truncate=False)

# ===== 6. 用户画像 =====
print("\n" + "=" * 50)
print("6. 用户偏好分类")

user_cat = behavior_df \
    .join(video_df.select("VideoID", "Category"), "VideoID") \
    .withColumn("s", score_expr) \
    .groupBy("UserID", "Category").agg(F.sum("s").alias("score")) \
    .withColumn("rank", F.row_number().over(Window.partitionBy("UserID").orderBy(F.desc("score")))) \
    .filter(F.col("rank") <= 2) \
    .select("UserID", "Category", "score")
user_cat.show(20, truncate=False)

# ===== 7. 导出 =====
print("\n导出结果...")
hot_recs.toPandas().to_csv(OUTPUT_DIR + "hot_recs.csv", index=False, encoding='utf-8-sig')
user_cat.toPandas().to_csv(OUTPUT_DIR + "user_prefs.csv", index=False, encoding='utf-8-sig')
print("已导出到 " + OUTPUT_DIR)

# 效果对比总结
print("\n" + "=" * 50)
print("7. 效果对比")
print(f"  ALS 协同过滤 RMSE: {rmse:.4f}")
print(f"  热度推荐: 所有人看到同样内容，适合首页热门榜")
print(f"  ALS 推荐: 千人千面，适合个人推荐流")
print(f"  热度推荐优点: 实现简单，冷启动友好，无需训练")
print(f"  ALS 推荐优点: 个性化程度高，能发现用户潜在兴趣")

spark.stop()
print("推荐算法模块完成")
