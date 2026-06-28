# 步骤4a：Kafka 实时事件生产者
# 模拟用户实时行为，发送到 Kafka topic video_events
# 如果没有 Kafka，会自动切换到 Socket 模式

import time
import json
import random
import numpy as np
import pandas as pd
from datetime import datetime

KAFKA_AVAILABLE = True  # 如果装了 Kafka 就改成 True
KAFKA_BOOTSTRAP = 'localhost:9092'
KAFKA_TOPIC = 'video_events'
SOCKET_HOST = 'localhost'
SOCKET_PORT = 9998
EVENTS_PER_SEC = 10

# 行为配置
ACTIONS = ['play', 'like', 'comment', 'share', 'favorite', 'follow']
ACTION_WEIGHTS = [0.55, 0.18, 0.10, 0.08, 0.06, 0.03]

# 加载已有数据
DATA_DIR = "D:/Spark/data/"
behavior_df = pd.read_csv(DATA_DIR + "video_behavior_history.csv")
video_df = pd.read_csv(DATA_DIR + "video_info.csv")

USER_IDS = sorted(behavior_df['UserID'].unique())
VIDEO_IDS = sorted(video_df['VideoID'].unique())
VIDEO_CATS = dict(zip(video_df['VideoID'], video_df['Category']))


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
        'Category': VIDEO_CATS.get(vid, ''),
    }


# ===== Kafka 模式 =====
def run_kafka():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: f"{v['UserID']},{v['VideoID']},{v['Action']},{v['WatchTime']},{v['EventTime']}".encode('utf-8')
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


# ===== Socket 模式（不需要 Kafka） =====
def run_socket():
    import socket

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((SOCKET_HOST, SOCKET_PORT))
    server.listen(1)
    print(f"Socket 服务器已启动: {SOCKET_HOST}:{SOCKET_PORT}")
    print("等待 Spark Streaming 连接...")

    conn, addr = server.accept()
    print(f"客户端已连接: {addr}")
    print(f"速率: {EVENTS_PER_SEC} 条/秒, 按 Ctrl+C 停止")

    cnt = 0
    t0 = time.time()
    while True:
        event = make_event()
        line = f"{event['UserID']},{event['VideoID']},{event['Action']},{event['WatchTime']},{event['EventTime']}\n"
        conn.send(line.encode('utf-8'))
        cnt += 1
        if cnt % 50 == 0:
            elapsed = time.time() - t0
            rate = cnt / elapsed if elapsed > 0 else 0
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 已发送 {cnt} 条, "
                  f"速率: {rate:.1f}/s, 最新: {event['UserID']} {event['Action']} {event['VideoID']}")
        time.sleep(1.0 / EVENTS_PER_SEC)


if __name__ == '__main__':
    print("=" * 50)
    print("实时事件生产者")
    print("=" * 50)
    if KAFKA_AVAILABLE:
        run_kafka()
    else:
        print("(Kafka 不可用，使用 Socket 模式)")
        run_socket()
