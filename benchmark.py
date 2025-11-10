import requests, json, time, random, psutil, os, pandas as pd
from pymongo import MongoClient
import redis
from neo4j import GraphDatabase
import copy
import statistics

# Tạo bản copy để lưu vào Redis

# ----------------- 0. Download data -----------------
url = "https://spine-mri-public-data.s3.ap-southeast-1.amazonaws.com/transformed/cleaned_mri_data.json"
print("Downloading data from S3...")
data_list = requests.get(url).json()
print(f"Downloaded {len(data_list)} records ✅")
data_for_mongo = copy.deepcopy(data_list)
results = []

# ----------------- Chuẩn bị 100 patient_id random duy nhất cho cả 3 DB -----------------
all_patient_ids = list(set(d['patient_id'] for d in data_list))
sample_ids = random.sample(all_patient_ids, min(100, len(all_patient_ids)))
print(f"Using the same {len(sample_ids)} random patient_id for all DBs ✅")

# ----------------- 1. MongoDB -----------------
print("\n=== MongoDB Benchmark ===")
mongo_client = MongoClient("mongodb://localhost:27017")
mdb = mongo_client["mri"]["metadata"]
mdb.drop()  # xóa collection cũ

# Insert tất cả scan vào 1 collection
start_time = time.time()
mdb.insert_many(data_for_mongo)
# Tạo index cho patient_id để query nhanh
mdb.create_index("patient_id")
t_insert = time.time() - start_time

# Case B – Query filter manufacturer + slice_thickness
start_time = time.time()
res_filter = list(mdb.find({"manufacturer": "SIEMENS", "slice_thickness": {"$lt": 5.0}}))
t_query_filter = time.time() - start_time

# Case C – Random 100 patient_id
start_time = time.time()
res_random_docs = list(mdb.find({"patient_id": {"$in": sample_ids}}))
res_random_count = len(res_random_docs)
t_query_random = time.time() - start_time

results.append({
    "DB": "MongoDB",
    "InsertTime": t_insert,
    "QueryFilterWithConditionTime": t_query_filter,
    "QueryFilterInListPatientIdTime": t_query_random,
    "Records": len(data_list),
    "Records_Filtered": len(res_filter),
    "Records_Random": res_random_count
})
print(f"MongoDB: Insert {t_insert:.2f}s, QueryFilterWithConditionTime {t_query_filter:.2f}s ({len(res_filter)} records), QueryFilterInListPatientIdTime {t_query_random:.2f}s ({res_random_count} records)")

# ----------------- 2. Redis Benchmark với list -----------------
print("\n=== Redis Benchmark ===")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
r.flushdb()

start_time = time.time()
# Gom các record theo patient_id
temp_dict = {}
for d in data_list:
    pid = d["patient_id"]
    temp_dict.setdefault(pid, []).append(json.dumps(d))

# Lưu vào Redis: key = patient_id, value = list Redis
for pid, records in temp_dict.items():
    r.rpush(f"mri:{pid}", *records)
t_insert = time.time() - start_time
print(f"Redis insert time: {t_insert:.2f}s")

# Case B – Filter manufacturer + slice_thickness
start_time = time.time()
res_filter = []
for key in r.keys("mri:*"):
    for rec in r.lrange(key, 0, -1):
        d = json.loads(rec)
        if d["manufacturer"] == "SIEMENS" and d["slice_thickness"] < 5.0:
            res_filter.append(d)
t_query_filter = time.time() - start_time
print(f"Redis: QueryFilterWithConditionTime {t_query_filter:.2f}s ({len(res_filter)} records)")

# Case C – Random 100 patient_id
keys = [f"mri:{pid}" for pid in sample_ids]
start_time = time.time()
res_random = []
for key in keys:
    for rec in r.lrange(key, 0, -1):
        res_random.append(json.loads(rec))
t_query_random = time.time() - start_time

results.append({
    "DB": "Redis",
    "InsertTime": t_insert,
    "QueryFilterWithConditionTime": t_query_filter,
    "QueryFilterInListPatientIdTime": t_query_random,
    "Records": len(data_list),
    "Records_Filtered": len(res_filter),
    "Records_Random": len(res_random)
})
print(f"Redis: Insert {t_insert:.2f}s, QueryFilterWithConditionTime {t_query_filter:.2f}s ({len(res_filter)} records), QueryFilterInListPatientIdTime {t_query_random:.2f}s ({len(res_random)} records)")

# ----------------- 3. Neo4j -----------------
print("\n=== Neo4j Benchmark ===")
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j","strongpass123"))
with driver.session() as session:
    # Xóa dữ liệu cũ
    session.run("MATCH (n) DETACH DELETE n")

    # ----------------- Insert: mỗi scan là 1 node Scan -----------------
    start_time = time.time()
    for d in data_list:
        session.run("""
            MERGE (p:Patient {id:$pid})
            CREATE (s:Scan {
                id: $sid,
                slice_thickness: $st,
                pixel_spacing: $px,
                manufacturer: $mf,
                modality: $modality,
                file_name: $fn,
                file_path: $fp
            })
            MERGE (p)-[:HAS_SCAN]->(s)
        """, pid=d["patient_id"],
             sid=d["series_id"],
             st=d["slice_thickness"],
             px=d["pixel_spacing"],
             mf=d["manufacturer"],
             modality=d["modality"],
             fn=d["file_name"],
             fp=d["file_path"])
    t_insert = time.time() - start_time

    # ----------------- Case B: Filter manufacturer + slice_thickness -----------------
    start_time = time.time()
    result = session.run("""
        MATCH (s:Scan)
        WHERE s.manufacturer='SIEMENS' AND s.slice_thickness < 5
        RETURN s
    """)
    res_filter = [record["s"] for record in result]  # mỗi node Scan thỏa điều kiện
    t_query_filter = time.time() - start_time

    # ----------------- Case C: Random 100 patient_id -----------------
    start_time = time.time()
    result = session.run("""
        MATCH (p:Patient)-[:HAS_SCAN]->(s)
        WHERE p.id IN $ids
        RETURN s
    """, ids=sample_ids)
    res_random = [record["s"] for record in result]
    t_query_random = time.time() - start_time

results.append({
    "DB": "Neo4j",
    "InsertTime": t_insert,
    "QueryFilterWithConditionTime": t_query_filter,
    "QueryFilterInListPatientIdTime": t_query_random,
    "Records": len(data_list),
    "Records_Filtered": len(res_filter),
    "Records_Random": len(res_random)
})
print(f"Neo4j: Insert {t_insert:.2f}s, QueryFilterWithConditionTime {t_query_filter:.2f}s ({len(res_filter)} records), QueryFilterInListPatientIdTime {t_query_random:.2f}s ({len(res_random)} records)")
print("\n===========================================")
# ----------------- Case D: 100 queries tuần tự theo patient_id -----------------
def benchmark_sequential_patient_query_mongodb(mdb, patient_ids):
    times = []
    for pid in patient_ids:
        start = time.time()
        res = list(mdb.find({"patient_id": pid}))
        times.append(time.time() - start)
    return times

def benchmark_sequential_patient_query_redis(r, patient_ids):
    times = []
    for pid in patient_ids:
        start = time.time()
        records = r.lrange(f"mri:{pid}", 0, -1)
        times.append(time.time() - start)
    return times

def benchmark_sequential_patient_query_neo4j(session, patient_ids):
    times = []
    for pid in patient_ids:
        start = time.time()
        result = session.run("""
            MATCH (p:Patient)-[:HAS_SCAN]->(s)
            WHERE p.id = $pid
            RETURN s
        """, pid=pid)
        _ = [record["s"] for record in result]
        times.append(time.time() - start)
    return times

# ----------------- Chạy benchmark cho MongoDB -----------------
mongo_times = benchmark_sequential_patient_query_mongodb(mdb, sample_ids)
print(f"MongoDB sequential queries (100): min={min(mongo_times):.4f}s, max={max(mongo_times):.4f}s, avg={statistics.mean(mongo_times):.4f}s")

# ----------------- Chạy benchmark cho Redis -----------------
redis_times = benchmark_sequential_patient_query_redis(r, sample_ids)
print(f"Redis sequential queries (100): min={min(redis_times):.4f}s, max={max(redis_times):.4f}s, avg={statistics.mean(redis_times):.4f}s")

# ----------------- Chạy benchmark cho Neo4j -----------------
with driver.session() as session:
    neo4j_times = benchmark_sequential_patient_query_neo4j(session, sample_ids)
print(f"Neo4j sequential queries (100): min={min(neo4j_times):.4f}s, max={max(neo4j_times):.4f}s, avg={statistics.mean(neo4j_times):.4f}s")

# ----------------- 4. Xuất CSV -----------------
df = pd.DataFrame(results)
df.to_csv("benchmark.csv", index=False)
print("\n✅ Benchmark completed. Results saved to benchmark.csv")
print(df)
