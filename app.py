import requests
import json
import time
from collections import defaultdict
from rediscluster import RedisCluster, RedisClusterException

# ======================================
# ⚙️ Cấu hình
# ======================================
URL = "https://spine-mri-public-data.s3.ap-southeast-1.amazonaws.com/transformed/cleaned_mri_data.json"

# Redis Cluster nodes
startup_nodes = [
    {"host": "127.0.0.1", "port": "7001"},
    {"host": "127.0.0.1", "port": "7002"},
    {"host": "127.0.0.1", "port": "7003"},
]

r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)

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
        # Hash tag {pid} để key cùng bệnh nhân nằm cùng node
        key = f"patient:{{{pid}}}:{sid}:{seid}"
        grouped[key].append(item)
    print(f"✅ Gom được {len(grouped)} nhóm dữ liệu.")
    # ======================================
    # 🧮 Tính dung lượng từng nhóm
    # ======================================
    total_size_bytes = 0
    for key, value in grouped.items():
        json_str = json.dumps(value)
        size_bytes = len(json_str.encode('utf-8'))
        size_mb = size_bytes / (1024 * 1024)
        total_size_bytes += size_bytes
        print(f"Key: {key}, Items: {len(value)}, Size: {size_mb:.2f} MB")

    total_size_mb = total_size_bytes / (1024 * 1024)
    print(f"\n🧠 Tổng dung lượng dự kiến cho {len(grouped)} nhóm: {total_size_mb:.2f} MB")
    return grouped


# ======================================
# 💾 Ghi trực tiếp từng key vào Redis
# ======================================
def save_to_redis_direct(grouped):
    total_keys = len(grouped)
    print(f"💾 Bắt đầu ghi {total_keys} keys vào Redis Cluster...")

    start_time = time.time()
    keys_written = 0

    for key, value in grouped.items():
        try:
            r.set(key, json.dumps(value))
            keys_written += 1
            if keys_written % 100 == 0 or keys_written == total_keys:
                print(f"  ✅ Đã ghi {keys_written}/{total_keys} keys")
        except Exception as e:
            print(f"❌ Lỗi khi ghi key {key}: {e}")

    duration = time.time() - start_time
    rate = total_keys / duration if duration > 0 else total_keys
    print(f"✅ Hoàn tất ghi {total_keys} keys trong {duration:.2f}s ({rate:.2f} keys/s)")

    print_redis_memory(r)

def print_redis_memory(r):
    try:
        mem_info = r.info("memory")  # Trả về dict theo node
        print("🧠 Memory info per node:")
        for node, info in mem_info.items():
            # info là dict chứa used_memory, used_memory_human, ...
            used_human = info.get("used_memory_human", "N/A")
            print(f"  Node {node}: {used_human}")
    except Exception as e:
        print("❌ Lỗi khi lấy memory info:", e)

# ======================================
# 🚀 MAIN
# ======================================
def main():
    data = fetch_data(URL)
    grouped = group_data(data)
    save_to_redis_direct(grouped)
    print("\n🎯 Hoàn tất ghi dữ liệu vào Redis Cluster.")


if __name__ == "__main__":
    main()
