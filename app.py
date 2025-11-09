import requests
import json
import time
from collections import defaultdict
from rediscluster import RedisCluster

# ======================================
# ⚙️ Cấu hình
# ======================================
URL = "https://spine-mri-public-data.s3.ap-southeast-1.amazonaws.com/transformed/cleaned_mri_data.json"

# Redis Cluster nodes
startup_nodes = [
    {"host": "127.0.0.1", "port": "7000"},
    {"host": "127.0.0.1", "port": "7001"},
    {"host": "127.0.0.1", "port": "7002"},
]

# Kết nối tới Redis Cluster
r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)


# ======================================
# 📥 Tải dữ liệu
# ======================================
def fetch_data(url):
    print("🔄 Đang tải dữ liệu...")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print(f"✅ Đã tải {len(data)} bản ghi.")
    return data


# ======================================
# 🧠 Gom nhóm dữ liệu
# ======================================
def group_data(data):
    grouped = defaultdict(list)
    for item in data:
        pid = item.get("patient_id")
        sid = item.get("study_id")
        seid = item.get("series_id")
        if not all([pid, sid, seid]):
            continue
        # Dùng hash tag {pid} để các key cùng bệnh nhân nằm cùng node
        key = f"patient:{{{pid}}}:{sid}:{seid}"
        grouped[key].append(item)
    print(f"✅ Gom được {len(grouped)} nhóm dữ liệu.")
    return grouped


# ======================================
# 💾 Lưu dữ liệu + benchmark
# ======================================
def save_to_redis_with_benchmark(grouped):
    total_keys = len(grouped)
    print(f"💾 Bắt đầu ghi {total_keys} nhóm vào Redis Cluster...")

    # --- Ghi + đo thời gian ---
    start_time = time.time()
    pipe = r.pipeline(transaction=False)
    count = 0
    batch_size = 500  # flush mỗi 500 keys để không quá tải network

    for key, value in grouped.items():
        pipe.set(key, json.dumps(value))
        count += 1
        if count % batch_size == 0:
            pipe.execute()
    pipe.execute()  # flush phần cuối

    duration = time.time() - start_time
    rate = total_keys / duration

    print(f"✅ Đã ghi {total_keys} keys trong {duration:.2f}s ({rate:.2f} keys/s)")

    # --- Dung lượng bộ nhớ ---
    mem_info = r.info("memory")
    print(f"🧠 Tổng dung lượng Redis đang dùng: {mem_info['used_memory_human']}")

    return duration, rate


# ======================================
# 📊 Thống kê phân bổ giữa các node
# ======================================
def cluster_distribution():
    print("\n📊 Phân bố dữ liệu giữa các node:")
    cluster_nodes = r.cluster_nodes()
    masters = [n for n in cluster_nodes.values() if n['role'] == 'master']
    for node in masters:
        node_id = node['id']
        addr = node['addr']
        node_info = r.info('memory', target_nodes=node_id)
        used = node_info[node_id]['used_memory_human']
        print(f"  - Node {addr}: dùng {used}")


# ======================================
# 🚀 MAIN
# ======================================
def main():
    data = fetch_data(URL)
    grouped = group_data(data)
    duration, rate = save_to_redis_with_benchmark(grouped)
    cluster_distribution()
    print("\n🎯 Hoàn tất benchmark Redis Cluster với dữ liệu thật.")


if __name__ == "__main__":
    main()
