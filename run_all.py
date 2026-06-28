# 一键运行脚本
# 用法: python run_all.py

import subprocess
import os
import time

BASE = os.path.dirname(os.path.abspath(__file__))
JAVA_HOME = "D:/Spark/jdk17/jdk-17.0.19+10"

def run(name, script):
    print(f"\n{'#'*50}")
    print(f"# {name}")
    print(f"{'#'*50}")
    env = os.environ.copy()
    env["JAVA_HOME"] = JAVA_HOME
    env["PATH"] = JAVA_HOME + "/bin;" + env.get("PATH", "")
    r = subprocess.run(["python", script], cwd=BASE, env=env)
    if r.returncode != 0:
        print(f"\n{name} 失败 (exit={r.returncode})")
    return r.returncode

if __name__ == '__main__':
    print("课题15: 短视频用户行为分析与内容推荐系统")
    print("注意: 实时处理需手动启动")
    print("  终端1: python 4_kafka_producer.py")
    print("  终端2: python 5_streaming.py")
    print()

    t0 = time.time()

    if run("步骤1: 生成数据", "1_generate_data.py") != 0:
        exit(1)
    if run("步骤2: 离线分析", "2_offline_analysis.py") != 0:
        exit(1)
    if run("步骤3: 推荐算法", "3_recommendation.py") != 0:
        exit(1)
    if run("步骤4: 可视化", "6_visualization.py") != 0:
        exit(1)

    t = time.time() - t0
    print(f"\n全部完成, 用时 {t:.1f}s")
    print(f"数据: {os.path.join(BASE, 'data/')}")
    print(f"结果: {os.path.join(BASE, 'output/')}")
    print(f"图表: {os.path.join(BASE, 'output/charts/')}")
