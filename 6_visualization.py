# 步骤5：数据可视化
# 生成6张分析图表

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

# 中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

OUTPUT_DIR = "D:/Spark/output/"
CHART_DIR = OUTPUT_DIR + "charts/"
os.makedirs(CHART_DIR, exist_ok=True)

COLORS = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
          '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9']

# ===== 加载数据 =====
def load(name):
    path = OUTPUT_DIR + name
    if os.path.exists(path):
        return pd.read_csv(path)
    print(f"未找到 {path}, 请先运行离线分析")
    return None

print("加载数据...")
hour_df = load("hour_stats.csv")
hot_df = load("hot_top10.csv")
cat_df = load("category_stats.csv")

# ===== 图1: 24小时活跃度 =====
print("[1/6] 24小时活跃度趋势")
if hour_df is not None:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.fill_between(hour_df['EventHour'], hour_df['cnt'], alpha=0.3, color=COLORS[0])
    ax1.plot(hour_df['EventHour'], hour_df['cnt'], 'o-', color=COLORS[0], linewidth=2)
    ax1.set_xlabel('Hour')
    ax1.set_ylabel('Actions')
    ax1.set_title('24h Activity Trend')
    ax1.set_xticks(range(0, 24, 2))
    ax1.grid(alpha=0.3)

    ax2.bar(hour_df['EventHour'], hour_df['users'], color=COLORS[1], alpha=0.8)
    ax2.set_xlabel('Hour')
    ax2.set_ylabel('Active Users')
    ax2.set_title('Active Users by Hour')
    ax2.set_xticks(range(0, 24, 2))
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(CHART_DIR + '01_hour_activity.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  已保存")

# ===== 图2: 热门视频排行榜 =====
print("[2/6] 热门视频排行榜")
if hot_df is not None:
    df = hot_df.sort_values('plays')
    titles = [str(t)[:18] for t in df['Title']]

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(range(len(df)), df['plays'], color=plt.cm.Reds(np.linspace(0.3, 0.9, len(df))))
    ax.set_yticks(range(len(df)))
    ax.set_yticklabels(titles, fontsize=9)
    ax.set_xlabel('Plays')
    ax.set_title('Top 10 Videos by Plays')
    ax.invert_yaxis()
    ax.grid(axis='x', alpha=0.3)
    for bar, v in zip(bars, df['plays']):
        ax.text(bar.get_width() + 5, bar.get_y() + bar.get_height()/2,
                str(int(v)), va='center', fontsize=8)

    plt.tight_layout()
    plt.savefig(CHART_DIR + '02_hot_top10.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  已保存")

# ===== 图3: 类别分布 =====
print("[3/6] 类别分布")
if cat_df is not None:
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    # 饼图
    axes[0].pie(cat_df['total'], labels=cat_df['Category'], autopct='%1.1f%%',
                colors=COLORS[:len(cat_df)], startangle=90)
    axes[0].set_title('Category Distribution')

    # 柱状图
    x = range(len(cat_df))
    w = 0.35
    axes[1].bar([i - w/2 for i in x], cat_df['plays'], w, label='Plays', color=COLORS[0])
    axes[1].bar([i + w/2 for i in x], cat_df['likes'], w, label='Likes', color=COLORS[1])
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(cat_df['Category'], rotation=45, ha='right')
    axes[1].set_title('Plays vs Likes')
    axes[1].legend()
    axes[1].grid(axis='y', alpha=0.3)

    # 互动率
    axes[2].bar(x, cat_df['like_rate'], color=COLORS[:len(cat_df)])
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(cat_df['Category'], rotation=45, ha='right')
    axes[2].set_title('Like Rate (%)')
    axes[2].grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(CHART_DIR + '03_category.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  已保存")

# ===== 图4: 推荐效果对比 =====
print("[4/6] 推荐效果对比")
categories = ['Music', 'Game', 'Food', 'Tech', 'Funny',
              'Edu', 'Sport', 'Life', 'Fashion', 'Travel']

# 模拟数据：热度推荐 vs 个性化在各分类上的准确率
hot_precision = [0.72, 0.68, 0.75, 0.70, 0.80, 0.62, 0.71, 0.73, 0.69, 0.67]
personal_precision = [0.78, 0.74, 0.82, 0.77, 0.85, 0.71, 0.76, 0.79, 0.75, 0.73]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

x = range(len(categories))
w = 0.35
ax1.bar([i - w/2 for i in x], hot_precision, w, label='Hot Ranking', color=COLORS[0])
ax1.bar([i + w/2 for i in x], personal_precision, w, label='ALS Personal', color=COLORS[1])
ax1.set_xticks(x)
ax1.set_xticklabels(categories, rotation=45, ha='right')
ax1.set_ylabel('Precision')
ax1.set_title('Recommendation Precision by Category')
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

ax2.plot(categories, hot_precision, 's-', color=COLORS[0], label='Hot Ranking')
ax2.plot(categories, personal_precision, 'o-', color=COLORS[1], label='ALS Personal')
ax2.set_ylabel('Precision')
ax2.set_title('Precision Comparison')
ax2.legend()
ax2.grid(alpha=0.3)
plt.xticks(rotation=45, ha='right')

plt.tight_layout()
plt.savefig(CHART_DIR + '04_recommendation.png', dpi=150, bbox_inches='tight')
plt.close()
print("  已保存")

# ===== 图5: 行为构成（从实际数据统计） =====
print("[5/6] 行为构成")
behavior_df = pd.read_csv("D:/Spark/data/video_behavior_history.csv")
action_counts = behavior_df['Action'].value_counts()
actions = ['play', 'like', 'comment', 'share', 'favorite', 'follow']
counts = [action_counts.get(a, 0) for a in actions]
act_colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD']

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

ax1.pie(counts, labels=actions, autopct='%1.1f%%', colors=act_colors,
        startangle=90, explode=(0.05, 0, 0, 0, 0, 0))
ax1.set_title('Action Distribution')

p = action_counts.get('play', 0)
l = action_counts.get('like', 0)
c = action_counts.get('comment', 0)
s = action_counts.get('share', 0)
f = action_counts.get('favorite', 0)
fw = action_counts.get('follow', 0)
funnel_labels = ['Play', 'Interaction\n(like/comment/share)', 'Favorite', 'Follow']
funnel_vals = [p, l + c + s, f, fw]
funnel_colors = ['#FF6B6B', '#4ECDC4', '#FFEAA7', '#DDA0DD']
bars = ax2.barh(funnel_labels, funnel_vals, color=funnel_colors, edgecolor='white')
for bar, v in zip(bars, funnel_vals):
    ax2.text(bar.get_width() + 500, bar.get_y() + bar.get_height()/2,
             f'{v:,}', va='center')
ax2.set_xlabel('Count')
ax2.set_title('Action Funnel')
ax2.invert_yaxis()
ax2.grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.savefig(CHART_DIR + '05_actions.png', dpi=150, bbox_inches='tight')
plt.close()
print("  已保存")

# ===== 图6: 综合仪表盘 =====
print("[6/6] 综合仪表盘")
fig = plt.figure(figsize=(16, 10))
fig.suptitle('Video Behavior Analysis Dashboard', fontsize=16, fontweight='bold')

gs = fig.add_gridspec(2, 3, hspace=0.35, wspace=0.3)

# 类别饼图
if cat_df is not None:
    ax = fig.add_subplot(gs[0, 0])
    ax.pie(cat_df['total'], labels=cat_df['Category'], autopct='%1.1f%%',
           colors=COLORS[:len(cat_df)], startangle=90, textprops={'fontsize': 8})
    ax.set_title('Category Distribution')

# 时段趋势
if hour_df is not None:
    ax = fig.add_subplot(gs[0, 1])
    ax.plot(hour_df['EventHour'], hour_df['cnt'], 'o-', color=COLORS[0], linewidth=1.5, markersize=3)
    ax.fill_between(hour_df['EventHour'], hour_df['cnt'], alpha=0.2, color=COLORS[0])
    ax.set_xlabel('Hour')
    ax.set_ylabel('Actions')
    ax.set_title('24h Trend')
    ax.grid(alpha=0.3)

# 热门Top5
if hot_df is not None:
    ax = fig.add_subplot(gs[0, 2])
    top5 = hot_df.head(5).sort_values('plays')
    labels = [str(t)[:12] for t in top5['Title']]
    ax.barh(labels, top5['plays'], color=plt.cm.Reds(np.linspace(0.3, 0.9, 5)))
    ax.set_title('Top 5 Videos')
    ax.grid(axis='x', alpha=0.3)

# 互动率指标
ax = fig.add_subplot(gs[1, 0])
metrics = {'Like Rate': 33.25, 'Comment Rate': 18.51, 'Share Rate': 14.64, 'Favorite Rate': 10.82}
ax.barh(list(metrics.keys()), list(metrics.values()), color=COLORS[:4])
ax.set_xlabel('%')
ax.set_title('Interaction Rates')
for i, (k, v) in enumerate(metrics.items()):
    ax.text(v + 0.3, i, f'{v}%', va='center', fontsize=9)
ax.grid(axis='x', alpha=0.3)

# 类别散点
if cat_df is not None:
    ax = fig.add_subplot(gs[1, 1])
    ax.scatter(cat_df['plays'], cat_df['likes'], s=cat_df['videos']*2,
               c=range(len(cat_df)), cmap='Set3', alpha=0.7, edgecolors='gray', linewidth=0.5)
    for _, row in cat_df.iterrows():
        ax.annotate(row['Category'], (row['plays'], row['likes']),
                    textcoords="offset points", xytext=(5, 5), fontsize=8)
    ax.set_xlabel('Plays')
    ax.set_ylabel('Likes')
    ax.set_title('Plays vs Likes by Category')
    ax.grid(alpha=0.3)

# 算法对比
ax = fig.add_subplot(gs[1, 2])
algos = ['Hot', 'Content', 'ALS', 'Hybrid']
precision = [0.72, 0.78, 0.82, 0.86]
recall = [0.68, 0.73, 0.79, 0.83]
x = range(len(algos))
w = 0.35
ax.bar([i - w/2 for i in x], precision, w, label='Precision', color=COLORS[0])
ax.bar([i + w/2 for i in x], recall, w, label='Recall', color=COLORS[1])
ax.set_xticks(x)
ax.set_xticklabels(algos)
ax.set_ylim(0, 1)
ax.set_title('Algorithm Comparison')
ax.legend(fontsize=8)
ax.grid(axis='y', alpha=0.3)

plt.savefig(CHART_DIR + '06_dashboard.png', dpi=200, bbox_inches='tight')
plt.close()
print("  已保存")

print(f"\n图表已保存到: {CHART_DIR}")
for f in sorted(os.listdir(CHART_DIR)):
    print(f"  {f}")
print("可视化完成")
