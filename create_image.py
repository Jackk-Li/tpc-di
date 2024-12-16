import matplotlib.pyplot as plt
import numpy as np

# 定义任务列表和每个SF下的执行时间（从图中读取的数据）
tasks = [
    'create_staging_schema',
    'load_txt_csv_to_staging',
    'load_finwire_to_staging',
    'parse_finwire',
    'convert_load_customermgmt',
    'create_master_schema',
    'load_master_static',
    'transform_load_dimcompany',
    'load_dimessages_dimcompany',
    'transform_load_dimbroker',
    'transform_load_prospect',
    'transform_load_dimcustomer',
    'load_dimessages_dimcustomer',
    'update_prospect',
    'transform_load_dimaccount',
    'transform_load_dimsecurity',
    'transform_load_dimtrade',
    'load_dimessages_dimtrade',
    'transform_load_financial',
    'transform_load_factcashbalance',
    'transform_load_factholdings',
    'transform_load_factwatches',
    'transform_load_factmarkethistory',
    'load_dimessages_dimtrade'
]

# 各个SF下各任务的执行时间（单位：秒）
sf_3_times = [
    1, 60, 7, 7, 146, 0, 2, 0, 3, 1, 2, 1, 1, 3, 1, 7, 68, 5, 132, 400, 106, 587, 287, 0
]

sf_6_times = [
    1, 197, 17, 22, 811, 0, 2, 0, 0, 0, 1, 1, 0, 6, 2, 13, 372, 4, 506, 883, 239, 1573, 394, 0
]

sf_9_times = [
    1, 187, 18, 16, 1030, 1, 2, 0, 0, 1, 2, 1, 1, 12, 2, 28, 1573, 8, 1422, 1765, 433, 2021, 191, 0
]

sf_12_times = [
    1, 304, 25, 37, 1762, 1, 2, 1, 0, 1, 3, 2, 1, 18, 3, 46, 1043, 16, 2528, 2267, 1005, 3120, 7, 0
]

# 设置图表样式
plt.style.use('seaborn')
plt.figure(figsize=(15, 10))

# 定义x轴数据点
scale_factors = [3, 6, 9, 12]

# 绘制每个任务的折线
colors = plt.cm.rainbow(np.linspace(0, 1, len(tasks)))
for i, task in enumerate(tasks):
    times = [sf_3_times[i], sf_6_times[i], sf_9_times[i], sf_12_times[i]]
    plt.plot(scale_factors, times, label=task, color=colors[i], marker='o')

# 设置图表属性
plt.title('Run Time (Sec) by each DAG Task & Scale Factor', fontsize=14, pad=20)
plt.xlabel('Scale Factor (SF)', fontsize=12)
plt.ylabel('Run Time (Sec)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)

# 设置坐标轴范围
plt.xlim(2, 13)
plt.ylim(0, 3500)

# 添加图例
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

# 调整布局以显示完整的图例
plt.tight_layout()

# 显示图表
plt.show()

# 找出耗时最长的任务
max_times = []
for i, task in enumerate(tasks):
    max_time = max(sf_3_times[i], sf_6_times[i], sf_9_times[i], sf_12_times[i])
    max_times.append((max_time, i, task))

# 排序并获取前5个最耗时的任务的索引
top_5_indices = sorted(range(len(max_times)), key=lambda k: max_times[k][0], reverse=True)[:5]
top_5_tasks = [tasks[i] for i in top_5_indices]

print("Removed tasks:", top_5_tasks)

# 过滤掉这5个任务的数据
filtered_tasks = [task for i, task in enumerate(tasks) if i not in top_5_indices]
filtered_sf_3_times = [time for i, time in enumerate(sf_3_times) if i not in top_5_indices]
filtered_sf_6_times = [time for i, time in enumerate(sf_6_times) if i not in top_5_indices]
filtered_sf_9_times = [time for i, time in enumerate(sf_9_times) if i not in top_5_indices]
filtered_sf_12_times = [time for i, time in enumerate(sf_12_times) if i not in top_5_indices]

# 设置图表样式
plt.style.use('seaborn')
plt.figure(figsize=(15, 10))

# 定义x轴数据点
scale_factors = [3, 6, 9, 12]

# 绘制每个任务的折线
colors = plt.cm.rainbow(np.linspace(0, 1, len(filtered_tasks)))
for i, task in enumerate(filtered_tasks):
    times = [filtered_sf_3_times[i], filtered_sf_6_times[i], filtered_sf_9_times[i], filtered_sf_12_times[i]]
    plt.plot(scale_factors, times, label=task, color=colors[i], marker='o')

# 设置图表属性
plt.title('Run Time (Sec) by each DAG Task & Scale Factor\n(Excluding Top 5 Time-Consuming Tasks)', fontsize=14, pad=20)
plt.xlabel('Scale Factor (SF)', fontsize=12)
plt.ylabel('Run Time (Sec)', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)

# 根据剩余数据重新设置y轴范围
max_time = max([max(filtered_sf_3_times), max(filtered_sf_6_times), 
                max(filtered_sf_9_times), max(filtered_sf_12_times)])
plt.xlim(2, 13)
plt.ylim(0, max_time * 1.1)  # 给顶部留出10%的空间

# 添加图例
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

# 调整布局以显示完整的图例
plt.tight_layout()

# 显示图表
plt.show()