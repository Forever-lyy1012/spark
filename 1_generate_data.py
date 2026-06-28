# 步骤1：生成模拟数据
# 生成 video_info.csv（视频信息）和 video_behavior_history.csv（用户行为记录）

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# 参数设置
NUM_USERS = 1000
NUM_VIDEOS = 500
NUM_RECORDS = 100000
OUTPUT_DIR = "D:/Spark/data/"
CATEGORIES = ['音乐', '游戏', '美食', '科技', '搞笑', '教育', '体育', '生活', '时尚', '旅行']
ACTIONS = ['play', 'like', 'comment', 'share', 'favorite', 'follow']
ACTION_WEIGHTS = [0.55, 0.18, 0.10, 0.08, 0.06, 0.03]

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===== 生成视频信息表 =====
print("生成视频信息表...")
np.random.seed(42)

video_ids = [f"V{i:04d}" for i in range(1, NUM_VIDEOS + 1)]
categories = np.random.choice(CATEGORIES, NUM_VIDEOS)
durations = np.random.randint(15, 600, NUM_VIDEOS)
uploader_ids = [f"U{np.random.randint(1, 101):03d}" for _ in range(NUM_VIDEOS)]

base_date = datetime(2025, 6, 1)
upload_dates = [(base_date - timedelta(days=np.random.randint(0, 365))).strftime('%Y-%m-%d')
                for _ in range(NUM_VIDEOS)]

# 简单标题模板
title_words = {
    '音乐': ['翻唱歌曲', '原创音乐', '吉他弹唱', '钢琴演奏', '电音remix'],
    '游戏': ['通关攻略', '精彩操作', '新游测评', '赛事集锦', '搞笑时刻'],
    '美食': ['家常做法', '探店美食', '烹饪教程', '深夜放毒', '减脂餐'],
    '科技': ['开箱评测', '上手体验', '科技资讯', '数码好物', 'AI工具推荐'],
    '搞笑': ['笑不活了', '今日份快乐', '沙雕合集', '反转名场面', '整蛊系列'],
    '教育': ['知识点讲解', '考研资料', '英语学习', '编程入门', '公开课'],
    '体育': ['比赛回放', '精彩进球', '健身教程', 'NBA集锦', '跑步训练'],
    '生活': ['日常Vlog', '收纳技巧', '好物分享', '家居改造', '生活妙招'],
    '时尚': ['穿搭分享', '美妆教程', '发型设计', '开箱试穿', 'OOTD'],
    '旅行': ['旅行攻略', '探店打卡', '自驾游记', '景点推荐', '穷游指南'],
}

titles = []
for cat, vid in zip(categories, video_ids):
    words = title_words[cat]
    title = words[np.random.randint(0, len(words))] + vid
    titles.append(title)

video_df = pd.DataFrame({
    'VideoID': video_ids,
    'Title': titles,
    'Category': categories,
    'Duration_sec': durations,
    'UploadDate': upload_dates,
    'UploaderID': uploader_ids,
})
video_df.to_csv(OUTPUT_DIR + 'video_info.csv', index=False, encoding='utf-8-sig')
print(f"已生成 {len(video_df)} 条视频信息")

# ===== 生成历史行为数据 =====
print("生成行为数据...")
np.random.seed(123)
random.seed(123)

# 每个视频属于哪个分类
video_cat = dict(zip(video_ids, categories))
cat_videos = {cat: [v for v in video_ids if video_cat[v] == cat] for cat in CATEGORIES}

# 每个用户偏好2-4个分类
user_prefs = {}
for u in range(1, NUM_USERS + 1):
    uid = f"U{u:04d}"
    n_pref = np.random.randint(2, 5)
    user_prefs[uid] = set(np.random.choice(CATEGORIES, size=n_pref, replace=False))

# 视频热度（Zipf分布，少数视频特别火）
video_pop = np.random.zipf(1.5, NUM_VIDEOS)
video_pop = video_pop / video_pop.sum()

records = []
base_date = datetime(2025, 1, 1)

for i in range(NUM_RECORDS):
    uid = f"U{np.random.randint(1, NUM_USERS + 1):04d}"

    # 70%概率选偏好分类的视频
    if np.random.random() < 0.7 and user_prefs[uid]:
        pref_cat = np.random.choice(list(user_prefs[uid]))
        vid = np.random.choice(cat_videos[pref_cat])
    else:
        vid = np.random.choice(video_ids, p=video_pop)

    action = np.random.choice(ACTIONS, p=ACTION_WEIGHTS)

    if action == 'play':
        dur = video_df.loc[video_df['VideoID'] == vid, 'Duration_sec'].values[0]
        watch_time = np.random.randint(5, max(6, int(dur * 1.2)))
    elif action in ('like', 'favorite', 'follow'):
        watch_time = 0
    else:
        watch_time = np.random.randint(0, 30)

    event_time = base_date + timedelta(
        days=np.random.randint(0, 180),
        hours=np.random.randint(0, 24),
        minutes=np.random.randint(0, 60),
        seconds=np.random.randint(0, 60)
    )

    records.append({
        'UserID': uid,
        'VideoID': vid,
        'Action': action,
        'WatchTime': int(watch_time),
        'EventTime': event_time.strftime('%Y-%m-%d %H:%M:%S'),
    })

    if (i + 1) % 25000 == 0:
        print(f"  已生成 {i+1}/{NUM_RECORDS}")

behavior_df = pd.DataFrame(records)
behavior_df.to_csv(OUTPUT_DIR + 'video_behavior_history.csv', index=False, encoding='utf-8-sig')
print(f"已生成 {len(behavior_df)} 条行为记录")
print(f"行为分布: \n{behavior_df['Action'].value_counts().to_string()}")
print("数据生成完成")
