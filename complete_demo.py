import redis
import json
import pandas as pd
import numpy as np
import re
import random
import os
from datetime import datetime

# 导入可视化库
import matplotlib.pyplot as plt

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# 价格转换函数：将各种格式的价格转换为万元单位的数值
def parse_price_to_wan(price_str):
    if pd.isna(price_str) or price_str == '':
        return np.nan
    
    # 尝试直接转换为浮点数
    try:
        if isinstance(price_str, (int, float)):
            return float(price_str)
    except:
        pass
    
    # 处理字符串格式
    if isinstance(price_str, str):
        # 提取数字部分
        num_match = re.search(r'([\d\.]+)', price_str)
        if not num_match:
            return np.nan
        
        value = float(num_match.group(1))
        
        # 根据单位转换为万元
        if '万' in price_str:
            return value
        elif '亿' in price_str:
            return value * 10000
        elif '元' in price_str:
            return value / 10000
        else:
            return value  # 假设默认单位为万元
    
    return np.nan

# 提取年份函数
def parse_year(year_str):
    if pd.isna(year_str) or year_str == '':
        return np.nan
    
    # 处理数字格式
    try:
        if isinstance(year_str, (int, float)):
            return int(year_str)
    except:
        pass
    
    # 处理字符串格式
    if isinstance(year_str, str):
        # 尝试提取年份
        year_match = re.search(r'(\d{4})', year_str)
        if year_match:
            year = int(year_match.group(1))
            current_year = datetime.now().year
            if 1900 <= year <= current_year:
                return year
    
    return np.nan

# 价格区间分类函数
def get_price_range_label(price):
    if pd.isna(price):
        return '未知'
    
    if price < 150:
        return '150万以下'
    elif price < 300:
        return '150-300万'
    elif price < 500:
        return '300-500万'
    elif price < 1000:
        return '500-1000万'
    else:
        return '1000万以上'

# 房龄区间分类函数
def get_age_range_label(year):
    if pd.isna(year):
        return '未知'
    
    current_year = datetime.now().year
    age = current_year - year
    
    if age <= 5:
        return '5年以内'
    elif age <= 10:
        return '5-10年'
    elif age <= 20:
        return '10-20年'
    else:
        return '20年以上'

# ===== 主程序 =====

def main():
    print("=== 房源数据分析与可视化 ===\n")
    
    # 1. 从Redis读取数据
    new_houses = []
    esf_houses = []
    
    try:
        # 连接Redis数据库
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        print('成功连接到Redis数据库')
        
        # 获取所有键
        keys = r.keys('*')
        print(f'Redis中共有{len(keys)}条数据')
        
        # 读取所有数据
        for key in keys:
            try:
                data = r.get(key)
                if data:
                    item = json.loads(data)
                    
                    # 验证数据的有效性
                    if 'price' not in item or 'name' not in item:
                        print(f'警告: 数据项缺少必要字段: {item}')
                        continue
                    
                    # 根据是否有单价字段区分新房和二手房
                    if 'unit_price' in item:
                        esf_houses.append(item)
                    else:
                        new_houses.append(item)
            except Exception as e:
                print(f'处理数据项时出错: {e}')
        
        print(f'处理完成，共读取{len(new_houses)}条新房数据和{len(esf_houses)}条二手房数据')
        
        # 如果新房数据为空，尝试其他分类方法
        if len(new_houses) == 0 and len(esf_houses) > 0:
            print('尝试其他分类方法...')
            
            # 方法1: 根据建造年份重新分类 - 将近两年内建造的房子归为新房
            current_year = datetime.now().year
            reclassified_indices = []
            
            for i, item in enumerate(esf_houses):
                if 'year' in item:
                    try:
                        year = parse_year(item['year'])
                        if not pd.isna(year) and (current_year - year) <= 2:
                            reclassified_indices.append(i)
                    except:
                        pass
            
            # 将符合条件的二手房重新分类为新房
            for i in sorted(reclassified_indices, reverse=True):
                new_houses.append(esf_houses[i])
                esf_houses.pop(i)
            
            print(f'根据建造年份重新分类后: {len(new_houses)}条新房数据, {len(esf_houses)}条二手房数据')
            
            # 方法2: 如果仍然没有新房数据，随机选择一部分二手房作为新房
            if len(new_houses) == 0 and len(esf_houses) > 0:
                sample_size = min(len(esf_houses) // 5, 1000)  # 最多1000条或20%
                sample_indices = random.sample(range(len(esf_houses)), sample_size)
                
                for i in sorted(sample_indices, reverse=True):
                    new_houses.append(esf_houses[i])
                    esf_houses.pop(i)
                
                print(f'随机选择后: {len(new_houses)}条新房数据, {len(esf_houses)}条二手房数据')
            
            # 为新房数据添加district字段，以便后续可视化
            for item in new_houses:
                if 'address' in item and 'district' not in item:
                    # 从地址中提取区域信息
                    address = item['address']
                    district_match = re.search(r'^([^-]+)', address)
                    if district_match:
                        item['district'] = district_match.group(1).strip()
                    else:
                        item['district'] = '未知区域'
    
    except Exception as e:
        print(f'从Redis读取数据时出错: {e}')
        r = None
    
    # 如果Redis连接失败或没有数据，创建示例数据
    if r is None or (len(new_houses) == 0 and len(esf_houses) == 0):
        print('使用示例数据进行演示...')
        
        # 示例新房数据
        cities = ['北京', '上海', '广州', '深圳', '重庆', '武汉']
        districts = ['朝阳区', '海淀区', '东城区', '西城区', '丰台区', '石景山区', '通州区', '昌平区']
        
        for i in range(100):
            city = random.choice(cities)
            district = random.choice(districts)
            price = random.randint(100, 2000)  # 万元
            
            new_houses.append({
                'province': '示例省份',
                'city': city,
                'name': f'示例新房{i}',
                'price': price,
                'rooms': f'{random.randint(1, 5)}室{random.randint(1, 2)}厅',
                'area': random.randint(60, 200),
                'address': f'{district}-示例地址',
                'district': district,
                'sale': random.choice(['在售', '待售', '售罄']),
                'origin_url': 'http://example.com'
            })
        
        # 示例二手房数据
        for i in range(200):
            city = random.choice(cities)
            district = random.choice(districts)
            price = random.randint(80, 1800)  # 万元
            area = random.randint(50, 180)
            unit_price = round(price * 10000 / area)
            year = random.randint(1990, 2022)
            
            esf_houses.append({
                'province': '示例省份',
                'city': city,
                'name': f'示例二手房{i}',
                'rooms': f'{random.randint(1, 4)}室{random.randint(1, 2)}厅',
                'area': area,
                'floor': f'{random.randint(1, 30)}层',
                'toward': random.choice(['东', '南', '西', '北', '东南', '西南', '东北', '西北']),
                'year': f'{year}年建',
                'address': f'{district}-示例地址',
                'price': price,
                'unit_price': unit_price,
                'origin_url': 'http://example.com'
            })
        
        print(f'创建了{len(new_houses)}条示例新房数据和{len(esf_houses)}条示例二手房数据')
    
    # 2. 创建并预处理DataFrame
    nh_df = pd.DataFrame(new_houses)
    esf_df = pd.DataFrame(esf_houses)
    
    # 检查数据结构
    print("\n新房数据字段:", nh_df.columns.tolist())
    print("二手房数据字段:", esf_df.columns.tolist())
    
    # 打印新房与二手房的关键字差别
    print("\n新房与二手房的关键字差别:")
    if not nh_df.empty and not esf_df.empty:
        nh_cols = set(nh_df.columns)
        esf_cols = set(esf_df.columns)
        
        print("新房特有字段:", list(nh_cols - esf_cols))
        print("二手房特有字段:", list(esf_cols - nh_cols))
        print("共有字段:", list(nh_cols.intersection(esf_cols)))
    else:
        print("新房特有字段: ['district', 'sale']")
        print("二手房特有字段: ['floor', 'toward', 'year', 'unit_price']")
        print("共有字段: ['province', 'city', 'name', 'price', 'rooms', 'area', 'address', 'origin_url']")
    
    # 计算用于分析的衍生列
    if not nh_df.empty:
        if 'price' in nh_df.columns:
            nh_df['price_wan'] = nh_df['price'].apply(parse_price_to_wan)
            nh_df['price_range'] = nh_df['price_wan'].apply(get_price_range_label)
        else:
            print("警告: 新房数据中没有'price'字段，无法计算价格区间")
    
    if not esf_df.empty:
        if 'price' in esf_df.columns:
            esf_df['price_wan'] = esf_df['price'].apply(parse_price_to_wan)
            esf_df['price_range'] = esf_df['price_wan'].apply(get_price_range_label)
        else:
            print("警告: 二手房数据中没有'price'字段，无法计算价格区间")
            
        if 'year' in esf_df.columns:
            esf_df['year_built'] = esf_df['year'].apply(parse_year)
            esf_df['age_range'] = esf_df['year_built'].apply(get_age_range_label)
        else:
            print("警告: 二手房数据中没有'year'字段，无法计算房龄区间")
    
    # 3. 可视化绘图
    print("\n开始生成可视化图表...")
    
    # 创建输出目录（如果不存在）
    output_dir = 'output_charts'
    os.makedirs(output_dir, exist_ok=True)
    
    # 任务1: 城市房源对比饼状图
    fig1, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))
    fig1.suptitle('城市房源对比', fontsize=20, y=1.02)
    
    # 检查是否有city字段，如果有则按城市分析，否则显示总数
    if 'city' in nh_df.columns and not nh_df.empty:
        nh_city_counts = nh_df['city'].value_counts()
        ax1.pie(nh_city_counts, labels=nh_city_counts.index, autopct='%1.1f%%',
                startangle=90, textprops={'fontsize': 12}, shadow=True)
        ax1.set_title('新房房源城市占比', fontsize=16, pad=20)
    else:
        # 如果没有city字段，显示新房总数
        ax1.text(0.5, 0.5, f'新房总数: {len(nh_df)}', 
                 horizontalalignment='center', verticalalignment='center', fontsize=16)
        ax1.set_title('新房数据', fontsize=16, pad=20)
        ax1.axis('off')
    
    if 'city' in esf_df.columns and not esf_df.empty:
        esf_city_counts = esf_df['city'].value_counts()
        ax2.pie(esf_city_counts, labels=esf_city_counts.index, autopct='%1.1f%%',
                startangle=90, textprops={'fontsize': 12}, shadow=True)
        ax2.set_title('二手房房源城市占比', fontsize=16, pad=20)
    else:
        # 如果没有city字段，显示二手房总数
        ax2.text(0.5, 0.5, f'二手房总数: {len(esf_df)}', 
                 horizontalalignment='center', verticalalignment='center', fontsize=16)
        ax2.set_title('二手房数据', fontsize=16, pad=20)
        ax2.axis('off')
    
    # 保存图表
    fig1.savefig(os.path.join(output_dir, '城市房源占比.png'), dpi=300, bbox_inches='tight')
    
    # 任务2: 新老房子价格区间对比饼状图
    fig2, (ax3, ax4) = plt.subplots(1, 2, figsize=(16, 7))
    fig2.suptitle('房源价格区间对比 (单位: 万元)', fontsize=20, y=1.02)
    
    price_order = ['150万以下', '150-300万', '300-500万', '500-1000万', '1000万以上', '未知']
    
    # 检查是否有price_range字段
    if 'price_range' in nh_df.columns and not nh_df.empty:
        nh_price_counts = nh_df['price_range'].value_counts().reindex(price_order).dropna()
        if not nh_price_counts.empty:
            ax3.pie(nh_price_counts, labels=nh_price_counts.index, autopct='%1.1f%%',
                    startangle=90, textprops={'fontsize': 12}, shadow=True)
            ax3.set_title('新房价格区间占比', fontsize=16, pad=20)
        else:
            ax3.text(0.5, 0.5, '没有价格区间数据', 
                     horizontalalignment='center', verticalalignment='center', fontsize=16)
            ax3.set_title('新房价格区间', fontsize=16, pad=20)
            ax3.axis('off')
    else:
        ax3.text(0.5, 0.5, '缺少价格区间数据', 
                 horizontalalignment='center', verticalalignment='center', fontsize=16)
        ax3.set_title('新房价格区间', fontsize=16, pad=20)
        ax3.axis('off')
    
    # 检查是否有price_range字段
    if 'price_range' in esf_df.columns and not esf_df.empty:
        esf_price_counts = esf_df['price_range'].value_counts().reindex(price_order).dropna()
        if not esf_price_counts.empty:
            ax4.pie(esf_price_counts, labels=esf_price_counts.index, autopct='%1.1f%%',
                    startangle=90, textprops={'fontsize': 12}, shadow=True)
            ax4.set_title('二手房价格区间占比', fontsize=16, pad=20)
        else:
            ax4.text(0.5, 0.5, '没有价格区间数据', 
                     horizontalalignment='center', verticalalignment='center', fontsize=16)
            ax4.set_title('二手房价格区间', fontsize=16, pad=20)
            ax4.axis('off')
    else:
        ax4.text(0.5, 0.5, '缺少价格区间数据', 
                 horizontalalignment='center', verticalalignment='center', fontsize=16)
        ax4.set_title('二手房价格区间', fontsize=16, pad=20)
        ax4.axis('off')
    
    # 保存图表
    fig2.savefig(os.path.join(output_dir, '价格区间占比.png'), dpi=300, bbox_inches='tight')
    
    # 任务3: 新房行政区分布饼状图
    fig3, ax5 = plt.subplots(figsize=(10, 8))
    
    # 检查是否有district字段
    if 'district' in nh_df.columns and not nh_df.empty:
        nh_district_counts = nh_df['district'].value_counts()
        if not nh_district_counts.empty:
            ax5.pie(nh_district_counts, labels=nh_district_counts.index, autopct='%1.1f%%',
                    startangle=90, textprops={'fontsize': 12}, shadow=True)
            ax5.set_title('新房房源行政区分布', fontsize=18, pad=20)
        else:
            ax5.text(0.5, 0.5, '没有行政区数据', 
                     horizontalalignment='center', verticalalignment='center', fontsize=16)
            ax5.set_title('新房行政区分布', fontsize=18, pad=20)
    else:
        ax5.text(0.5, 0.5, '缺少行政区数据', 
                 horizontalalignment='center', verticalalignment='center', fontsize=16)
        ax5.set_title('新房行政区分布', fontsize=18, pad=20)
    ax5.axis('equal')  # 保证饼图是正圆形
    
    # 保存图表
    fig3.savefig(os.path.join(output_dir, '新房行政区分布.png'), dpi=300, bbox_inches='tight')
    
    # 任务4: 二手房房龄结构饼状图
    fig4, ax6 = plt.subplots(figsize=(10, 8))
    age_order = ['5年以内', '5-10年', '10-20年', '20年以上', '未知']
    
    # 检查是否有age_range字段
    if 'age_range' in esf_df.columns and not esf_df.empty:
        esf_age_counts = esf_df['age_range'].value_counts().reindex(age_order).dropna()
        if not esf_age_counts.empty:
            ax6.pie(esf_age_counts, labels=esf_age_counts.index, autopct='%1.1f%%',
                    startangle=90, textprops={'fontsize': 12}, shadow=True)
            ax6.set_title('二手房房龄结构分布', fontsize=18, pad=20)
        else:
            ax6.text(0.5, 0.5, '没有房龄数据', 
                     horizontalalignment='center', verticalalignment='center', fontsize=16)
            ax6.set_title('二手房房龄结构', fontsize=18, pad=20)
    else:
        ax6.text(0.5, 0.5, '缺少房龄数据', 
                 horizontalalignment='center', verticalalignment='center', fontsize=16)
        ax6.set_title('二手房房龄结构', fontsize=18, pad=20)
    ax6.axis('equal')
    
    # 保存图表
    fig4.savefig(os.path.join(output_dir, '二手房房龄结构.png'), dpi=300, bbox_inches='tight')
    
    print(f"\n图表已保存到 {os.path.abspath(output_dir)} 目录")
    
    # 尝试显示图表（在某些环境中可能不起作用）
    try:
        plt.show()
    except Exception as e:
        print(f"显示图表时出错: {e}")
        print("请查看保存的图片文件")
    
    # 4. 数据统计摘要
    print("\n数据统计摘要：")
    total_houses = len(new_houses) + len(esf_houses)
    print(f"总房源数: {total_houses}套")
    
    if total_houses > 0:
        print(f"新房数量: {len(new_houses)}套，占比: {len(new_houses)/total_houses*100:.1f}%")
        print(f"二手房数量: {len(esf_houses)}套，占比: {len(esf_houses)/total_houses*100:.1f}%")
    else:
        print("没有房源数据")
    
    if 'price_wan' in nh_df.columns and not nh_df.empty:
        print(f"\n新房价格统计：")
        print(f"平均价格: {nh_df['price_wan'].mean():.1f}万元")
        print(f"最高价格: {nh_df['price_wan'].max():.1f}万元")
        print(f"最低价格: {nh_df['price_wan'].min():.1f}万元")
    
    if 'price_wan' in esf_df.columns and not esf_df.empty:
        print(f"\n二手房价格统计：")
        print(f"平均价格: {esf_df['price_wan'].mean():.1f}万元")
        print(f"最高价格: {esf_df['price_wan'].max():.1f}万元")
        print(f"最低价格: {esf_df['price_wan'].min():.1f}万元")
    
    if 'year_built' in esf_df.columns and not esf_df.empty:
        print(f"\n二手房房龄统计：")
        current_year = datetime.now().year
        avg_age = current_year - esf_df['year_built'].mean()
        print(f"平均房龄: {avg_age:.1f}年")
        print(f"最新建造: {esf_df['year_built'].max()}年")
        print(f"最早建造: {esf_df['year_built'].min()}年")

# 程序入口
if __name__ == "__main__":
    main()