# 步骤4a：Kafka 实时事件生产者
# 模拟用户实时行为，发送到 Kafka topic video_events

import time
import numpy as np
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'video_events'
EVENTS_PER_SEC = 10

ACTIONS = ['play', 'like', 'comment', 'share', 'favorite', 'follow']
ACTION_WEIGHTS = [0.55, 0.18, 0.10, 0.08, 0.06, 0.03]

# 加载用户和视频池
DATA_DIR = "D:/Spark/data/"
behavior_df = pd.read_csv(DATA_DIR + "video_behavior_history.csv")
video_df = pd.read_csv(DATA_DIR + "video_info.csv")
USER_IDS = sorted(behavior_df['UserID'].unique())
VIDEO_IDS = sorted(video_df['VideoID'].unique())


def make_event():
    """随机生成一条行为事件"""
    uid = np.random.choice(USER_IDS)
    vid = np.random.choice(VIDEO_IDS)
    action = np.random.choice(ACTIONS, p=ACTION_WEIGHTS)

    if action == 'play':
        wt = np.random.randint(5, 300)
    elif action in ('like', 'favorite', 'follow'):
        wt = 0
    else:
        wt = np.random.randint(0, 30)

    return {
        'UserID': uid,
        'VideoID': vid,
        'Action': action,
        'WatchTime': int(wt),
        'EventTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


if __name__ == '__main__':
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v:
            f"{v['UserID']},{v['VideoID']},{v['Action']},{v['WatchTime']},{v['EventTime']}"
            .encode('utf-8')
    )
    print(f"Kafka 已连接: {KAFKA_BOOTSTRAP}, Topic: {KAFKA_TOPIC}")
    print(f"速率: {EVENTS_PER_SEC} 条/秒, 按 Ctrl+C 停止")

    cnt = 0
    t0 = time.time()
    while True:
        event = make_event()
        producer.send(KAFKA_TOPIC, value=event)
        cnt += 1
        if cnt % 50 == 0:
            elapsed = time.time() - t0
            rate = cnt / elapsed if elapsed > 0 else 0
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 已发送 {cnt} 条, "
                  f"速率: {rate:.1f}/s, 最新: {event['UserID']} {event['Action']} {event['VideoID']}")
        time.sleep(1.0 / EVENTS_PER_SEC)
