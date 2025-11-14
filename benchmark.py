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
mongo_client = MongoClient("mongodb://localhost:27000")
mdb = mongo_client["mri"]["metadata"]
mdb.drop()  # xóa collection cũ

# Insert tất cả scan vào 1 collection
start_time = time.time()
mdb.insert_many(data_for_mongo)
# Tạo indexes để query nhanh
mdb.create_index("patient_id")
mdb.create_index("file_name")
mdb.create_index("manufacturer")
mdb.create_index("slice_thickness")
t_insert = time.time() - start_time

# ----------------- Use Case A: Simple Lookups (Redis Strength) -----------------
# A1: Get single image by file_name
sample_file_name = data_list[0]["file_name"]
start_time = time.time()
res_a1 = mdb.find_one({"file_name": sample_file_name})
t_query_a1 = time.time() - start_time

# A2: Get all images for a patient
sample_patient_id = sample_ids[0]
start_time = time.time()
res_a2 = list(mdb.find({"patient_id": sample_patient_id}))
t_query_a2 = time.time() - start_time

# ----------------- Use Case B: Multi-Hop Relationship Queries (Neo4j Strength) -----------------
# B: Find patients connected through shared Studies or Series (multi-hop traversal)
# This requires traversing: Patient1 -> Study -> Series -> (shared) <- Series <- Study <- Patient2
start_time = time.time()
# Get studies and series for the sample patient
sample_patient_data = list(mdb.find({"patient_id": sample_patient_id}))
sample_studies = set(img.get("study_id", img.get("series_id", "")) for img in sample_patient_data)
sample_series = set(img["series_id"] for img in sample_patient_data)

# Find other patients who share the same studies or series (multi-hop relationship)
# Step 1: Find images in shared studies
shared_study_images = list(mdb.find({
    "study_id": {"$in": list(sample_studies)},
    "patient_id": {"$ne": sample_patient_id}
}))
# Step 2: Find images in shared series
shared_series_images = list(mdb.find({
    "series_id": {"$in": list(sample_series)},
    "patient_id": {"$ne": sample_patient_id}
}))

# Get unique patient_ids connected through shared studies or series
connected_patient_ids = set()
connected_patient_ids.update(img["patient_id"] for img in shared_study_images)
connected_patient_ids.update(img["patient_id"] for img in shared_series_images)
res_b = list(connected_patient_ids)
t_query_b = time.time() - start_time

# ----------------- Use Case C: Complex Multi-Level Aggregation & Grouping (MongoDB Strength) -----------------
# C: Multi-level grouping by manufacturer AND slice_thickness with complex statistics
start_time = time.time()
res_c = list(mdb.aggregate([
    # First level: Group by manufacturer and slice_thickness
    {"$group": {
        "_id": {
            "manufacturer": "$manufacturer",
            "slice_thickness": "$slice_thickness"
        },
        "image_count": {"$sum": 1},
        "avg_pixel_spacing_x": {
            "$avg": {"$arrayElemAt": ["$pixel_spacing", 0]}
        },
        "min_pixel_spacing_x": {
            "$min": {"$arrayElemAt": ["$pixel_spacing", 0]}
        },
        "max_pixel_spacing_x": {
            "$max": {"$arrayElemAt": ["$pixel_spacing", 0]}
        },
        "stddev_slice_thickness": {
            "$stdDevPop": "$slice_thickness"
        },
        "unique_patients": {"$addToSet": "$patient_id"},
        "unique_studies": {"$addToSet": "$study_id"},
        "unique_series": {"$addToSet": "$series_id"}
    }},
    # Add computed fields
    {"$addFields": {
        "patient_count": {"$size": "$unique_patients"},
        "study_count": {"$size": "$unique_studies"},
        "series_count": {"$size": "$unique_series"},
        "avg_images_per_patient": {
            "$divide": ["$image_count", {"$size": "$unique_patients"}]
        }
    }},
    # Second level: Group by manufacturer to aggregate across slice_thickness values
    {"$group": {
        "_id": "$_id.manufacturer",
        "total_images": {"$sum": "$image_count"},
        "total_patients": {"$sum": "$patient_count"},
        "slice_thickness_groups": {"$push": {
            "slice_thickness": "$_id.slice_thickness",
            "image_count": "$image_count",
            "avg_pixel_spacing_x": "$avg_pixel_spacing_x",
            "stddev_slice_thickness": "$stddev_slice_thickness",
            "patient_count": "$patient_count"
        }},
        "overall_avg_pixel_spacing_x": {"$avg": "$avg_pixel_spacing_x"},
        "min_slice_thickness": {"$min": "$_id.slice_thickness"},
        "max_slice_thickness": {"$max": "$_id.slice_thickness"}
    }},
    {"$sort": {"total_images": -1}}
]))
t_query_c = time.time() - start_time

# ----------------- Use Case D: Complex Multi-Condition Queries -----------------
# D: Filter by manufacturer AND slice_thickness AND pixel_spacing range
start_time = time.time()
res_d = list(mdb.find({
    "manufacturer": "SIEMENS",
    "slice_thickness": {"$lt": 5.0},
    "$expr": {
        "$and": [
            {"$gte": [{"$arrayElemAt": ["$pixel_spacing", 0]}, 0.6]},
            {"$lte": [{"$arrayElemAt": ["$pixel_spacing", 0]}, 0.7]}
        ]
    }
}))
t_query_d = time.time() - start_time

results.append({
    "DB": "MongoDB",
    "InsertTime": t_insert,
    "UseCaseA_SimpleLookup_Time": t_query_a1 + t_query_a2,
    "UseCaseB_Relationship_Time": t_query_b,
    "UseCaseC_Aggregation_Time": t_query_c,
    "UseCaseD_ComplexFilter_Time": t_query_d,
    "Records": len(data_list),
    "UseCaseA_Records": len(res_a2) if res_a2 else 0,
    "UseCaseB_Records": len(res_b) if isinstance(res_b, (list, set)) else 1,
    "UseCaseC_Groups": len(res_c),
    "UseCaseD_Records": len(res_d)
})
print(f"MongoDB: Insert {t_insert:.2f}s | A:SimpleLookup {t_query_a1+t_query_a2:.3f}s | B:Relationship {t_query_b:.3f}s | C:Aggregation {t_query_c:.3f}s | D:ComplexFilter {t_query_d:.3f}s")

# ----------------- 2. Redis Benchmark với Hashes + Sets (Optimized) -----------------
print("\n=== Redis Benchmark ===")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
r.flushdb()

start_time = time.time()
# Use pipeline for batch operations
pipe = r.pipeline()
pipeline_count = 0
for d in data_list:
    file_name = d["file_name"]
    pid = d["patient_id"]
    
    # Handle pixel_spacing
    pixel_spacing = d["pixel_spacing"]
    px_x = pixel_spacing[0] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 0 else pixel_spacing
    px_y = pixel_spacing[1] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 1 else pixel_spacing
    
    # Store image as hash
    pipe.hset(f"image:{file_name}", mapping={
        "patient_id": pid,
        "study_id": d.get("study_id", ""),
        "series_id": d["series_id"],
        "slice_thickness": str(d["slice_thickness"]),
        "pixel_spacing_x": str(px_x),
        "pixel_spacing_y": str(px_y),
        "manufacturer": d["manufacturer"],
        "modality": d["modality"],
        "file_path": d["file_path"]
    })
    
    # Add to patient index
    pipe.sadd(f"patient:{pid}:images", file_name)
    
    # Add to manufacturer index
    pipe.sadd(f"manufacturer:{d['manufacturer']}:images", file_name)
    
    # Add to slice_thickness index
    slice_thickness_key = f"slice_thickness:{d['slice_thickness']}"
    pipe.sadd(slice_thickness_key, file_name)
    
    pipeline_count += 4  # hset + 3 sadds (patient, manufacturer, slice_thickness)
    # Execute pipeline in batches of 1000 commands to avoid memory issues
    if pipeline_count >= 1000:
        pipe.execute()
        pipe = r.pipeline()
        pipeline_count = 0

# Execute remaining commands
if pipeline_count > 0:
    pipe.execute()

t_insert = time.time() - start_time
print(f"Redis insert time: {t_insert:.2f}s")

# ----------------- Use Case A: Simple Lookups (Redis Strength) -----------------
# A1: Get single image by file_name (O(1) hash lookup)
start_time = time.time()
res_a1 = r.hgetall(f"image:{sample_file_name}")
t_query_a1 = time.time() - start_time

# A2: Get all images for a patient (Set lookup + Hash retrieval)
start_time = time.time()
file_names = r.smembers(f"patient:{sample_patient_id}:images") or set()
pipe = r.pipeline()
for file_name in file_names:
    pipe.hgetall(f"image:{file_name}")
res_a2 = [dict(r) for r in pipe.execute() if r]
t_query_a2 = time.time() - start_time

# ----------------- Use Case B: Multi-Hop Relationship Queries (Neo4j Strength) -----------------
# B: Find patients connected through shared Studies or Series (multi-hop traversal)
# Redis doesn't have native relationships, so we simulate multi-hop with multiple lookups
start_time = time.time()
# Step 1: Get images for the sample patient
file_names_b = r.smembers(f"patient:{sample_patient_id}:images") or set()
pipe = r.pipeline()
for file_name in file_names_b:
    pipe.hmget(f"image:{file_name}", "study_id", "series_id")
images_b = pipe.execute()

# Extract studies and series IDs
sample_studies = set()
sample_series = set()
for img_data in images_b:
    if img_data and img_data[0]:
        sample_studies.add(img_data[0])
    if img_data and img_data[1]:
        sample_series.add(img_data[1])

# Step 2: Find all images in shared studies (requires scanning all images)
# Step 3: Find all images in shared series
# Since Redis doesn't have indexes on study_id/series_id, we need to scan or use alternative approach
# We'll need to fetch all images and filter (inefficient but necessary for multi-hop simulation)
connected_patient_ids = set()

# Get all image keys once (needed for both shared studies and series)
all_image_keys = []
if sample_studies or sample_series:
    for key in r.scan_iter(match="image:*"):
        all_image_keys.append(key)

# For shared studies: scan all images and check study_id
if sample_studies and all_image_keys:
    # Fetch study_id and patient_id for all images
    pipe = r.pipeline()
    for key in all_image_keys:
        pipe.hmget(key, "study_id", "patient_id")
    all_images_data = pipe.execute()
    
    # Filter for shared studies
    for key, img_data in zip(all_image_keys, all_images_data):
        if img_data and img_data[0] in sample_studies and img_data[1] and img_data[1] != sample_patient_id:
            connected_patient_ids.add(img_data[1])

# For shared series: similar approach
if sample_series and all_image_keys:
    pipe = r.pipeline()
    for key in all_image_keys:
        pipe.hmget(key, "series_id", "patient_id")
    all_images_data = pipe.execute()
    
    # Filter for shared series
    for key, img_data in zip(all_image_keys, all_images_data):
        if img_data and img_data[0] in sample_series and img_data[1] and img_data[1] != sample_patient_id:
            connected_patient_ids.add(img_data[1])

res_b = list(connected_patient_ids)
t_query_b = time.time() - start_time

# ----------------- Use Case C: Complex Multi-Level Aggregation & Grouping (MongoDB Strength) -----------------
# C: Multi-level grouping by manufacturer AND slice_thickness with complex statistics
# Redis doesn't have native aggregation, so we fetch and calculate manually (very inefficient)
start_time = time.time()
# Get all manufacturers
manufacturers = set()
for key in r.scan_iter(match="manufacturer:*:images"):
    manufacturer = key.split(":")[1]
    manufacturers.add(manufacturer)

# Get all slice_thickness values
slice_thicknesses = set()
for key in r.scan_iter(match="slice_thickness:*"):
    try:
        thickness = float(key.split(":")[1])
        slice_thicknesses.add(thickness)
    except (ValueError, IndexError):
        continue

# Multi-level grouping: manufacturer -> slice_thickness
# This requires fetching all images and grouping in Python (very slow)
res_c = []
for manufacturer in manufacturers:
    manufacturer_files = r.smembers(f"manufacturer:{manufacturer}:images") or set()
    if not manufacturer_files:
        continue
    
    # Fetch all image data for this manufacturer
    pipe = r.pipeline()
    for file_name in manufacturer_files:
        pipe.hmget(f"image:{file_name}", "slice_thickness", "pixel_spacing_x", "patient_id", "study_id", "series_id")
    all_images_data = pipe.execute()
    
    # Group by slice_thickness
    thickness_groups = {}
    for file_name, img_data in zip(manufacturer_files, all_images_data):
        if not img_data or not img_data[0]:
            continue
        thickness = float(img_data[0])
        if thickness not in thickness_groups:
            thickness_groups[thickness] = {
                "images": [],
                "pixel_spacing_x_values": [],
                "patients": set(),
                "studies": set(),
                "series": set()
            }
        
        thickness_groups[thickness]["images"].append(file_name)
        if img_data[1]:
            thickness_groups[thickness]["pixel_spacing_x_values"].append(float(img_data[1]))
        if img_data[2]:
            thickness_groups[thickness]["patients"].add(img_data[2])
        if img_data[3]:
            thickness_groups[thickness]["studies"].add(img_data[3])
        if img_data[4]:
            thickness_groups[thickness]["series"].add(img_data[4])
    
    # Calculate statistics for each slice_thickness group
    slice_thickness_groups = []
    total_images = 0
    total_patients = set()
    all_pixel_spacing = []
    
    for thickness, group_data in thickness_groups.items():
        image_count = len(group_data["images"])
        total_images += image_count
        total_patients.update(group_data["patients"])
        
        avg_px_x = sum(group_data["pixel_spacing_x_values"]) / len(group_data["pixel_spacing_x_values"]) if group_data["pixel_spacing_x_values"] else 0
        all_pixel_spacing.append(avg_px_x)
        
        # Calculate stddev manually
        px_values = group_data["pixel_spacing_x_values"]
        if len(px_values) > 1:
            mean_px = sum(px_values) / len(px_values)
            variance = sum((x - mean_px) ** 2 for x in px_values) / len(px_values)
            stddev_px = variance ** 0.5
        else:
            stddev_px = 0
        
        slice_thickness_groups.append({
            "slice_thickness": thickness,
            "image_count": image_count,
            "avg_pixel_spacing_x": avg_px_x,
            "stddev_slice_thickness": 0,  # Would need to calculate from thickness values
            "patient_count": len(group_data["patients"])
        })
    
    overall_avg_px = sum(all_pixel_spacing) / len(all_pixel_spacing) if all_pixel_spacing else 0
    
    res_c.append({
        "manufacturer": manufacturer,
        "total_images": total_images,
        "total_patients": len(total_patients),
        "slice_thickness_groups": slice_thickness_groups,
        "overall_avg_pixel_spacing_x": overall_avg_px,
        "min_slice_thickness": min(thickness_groups.keys()) if thickness_groups else 0,
        "max_slice_thickness": max(thickness_groups.keys()) if thickness_groups else 0
    })

res_c.sort(key=lambda x: x["total_images"], reverse=True)
t_query_c = time.time() - start_time

# ----------------- Use Case D: Complex Multi-Condition Queries -----------------
# D: Filter by manufacturer AND slice_thickness AND pixel_spacing range
start_time = time.time()
# Get files matching manufacturer
siemens_files = r.smembers("manufacturer:SIEMENS:images") or set()

# Get files matching slice_thickness < 5.0
slice_keys = []
for key in r.scan_iter(match="slice_thickness:*"):
    try:
        thickness = float(key.split(":")[1])
        if thickness < 5.0:
            slice_keys.append(key)
    except (ValueError, IndexError):
        continue

thickness_files = set()
if slice_keys:
    pipe = r.pipeline()
    for key in slice_keys:
        pipe.smembers(key)
    for result_set in pipe.execute():
        thickness_files.update(result_set)

# Intersect manufacturer and slice_thickness
matching_files = siemens_files & thickness_files

# Fetch and filter by pixel_spacing range
res_d = []
if matching_files:
    pipe = r.pipeline()
    for file_name in matching_files:
        pipe.hmget(f"image:{file_name}", "pixel_spacing_x", "pixel_spacing_y", "manufacturer", "slice_thickness", "file_name")
    results = pipe.execute()
    for result in results:
        if result and result[0]:  # pixel_spacing_x exists
            px_x = float(result[0])
            if 0.6 <= px_x <= 0.7:
                res_d.append({
                    "file_name": result[4],
                    "manufacturer": result[2],
                    "slice_thickness": float(result[3]) if result[3] else 0,
                    "pixel_spacing_x": px_x
                })
t_query_d = time.time() - start_time

results.append({
    "DB": "Redis",
    "InsertTime": t_insert,
    "UseCaseA_SimpleLookup_Time": t_query_a1 + t_query_a2,
    "UseCaseB_Relationship_Time": t_query_b,
    "UseCaseC_Aggregation_Time": t_query_c,
    "UseCaseD_ComplexFilter_Time": t_query_d,
    "Records": len(data_list),
    "UseCaseA_Records": len(res_a2),
    "UseCaseB_Records": len(res_b) if isinstance(res_b, (list, set)) else 1,
    "UseCaseC_Groups": len(res_c),
    "UseCaseD_Records": len(res_d)
})
print(f"Redis: Insert {t_insert:.2f}s | A:SimpleLookup {t_query_a1+t_query_a2:.3f}s | B:Relationship {t_query_b:.3f}s | C:Aggregation {t_query_c:.3f}s | D:ComplexFilter {t_query_d:.3f}s")

# ----------------- 3. Neo4j -----------------
print("\n=== Neo4j Benchmark ===")
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j","strongpass123"))
with driver.session() as session:
    # Xóa dữ liệu cũ
    session.run("MATCH (n) DETACH DELETE n")
    
    # ----------------- Create indexes for faster MERGE operations -----------------
    print("Creating indexes for faster inserts...")
    # Create indexes on properties used in MERGE operations to speed up lookups
    session.run("CREATE INDEX patient_id_idx IF NOT EXISTS FOR (p:Patient) ON (p.patient_id)")
    session.run("CREATE INDEX study_id_idx IF NOT EXISTS FOR (s:Study) ON (s.study_id)")
    session.run("CREATE INDEX series_id_idx IF NOT EXISTS FOR (sr:Series) ON (sr.series_id)")
    print("Indexes created ✅")

    # ----------------- Insert: Full hierarchy Patient->Study->Series->Image (Batched) -----------------
    start_time = time.time()
    
    # Batch insert for better performance
    batch_size = 1000
    batch = []
    
    for d in data_list:
        # Handle pixel_spacing as array
        pixel_spacing = d["pixel_spacing"]
        px_x = pixel_spacing[0] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 0 else pixel_spacing
        px_y = pixel_spacing[1] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 1 else pixel_spacing
        
        batch.append({
            "pid": d["patient_id"],
            "study_id": d.get("study_id", d["series_id"]),  # Use series_id as fallback if study_id doesn't exist
            "series_id": d["series_id"],
            "file_name": d["file_name"],
            "file_path": d["file_path"],
            "slice_thickness": d["slice_thickness"],
            "px_x": px_x,
            "px_y": px_y,
            "manufacturer": d["manufacturer"],
            "modality": d["modality"]
        })
        
        # Execute batch when full
        if len(batch) >= batch_size:
            session.run("""
                UNWIND $batch AS record
                MERGE (p:Patient {patient_id: record.pid})
                MERGE (study:Study {study_id: record.study_id})
                MERGE (p)-[:HAS_STUDY]->(study)
                MERGE (series:Series {series_id: record.series_id})
                MERGE (study)-[:HAS_SERIES]->(series)
                CREATE (img:Image {
                    file_name: record.file_name,
                    file_path: record.file_path,
                    slice_thickness: record.slice_thickness,
                    pixel_spacing_x: record.px_x,
                    pixel_spacing_y: record.px_y,
                    manufacturer: record.manufacturer,
                    modality: record.modality
                })
                CREATE (series)-[:CONTAINS]->(img)
            """, batch=batch)
            batch = []
    
    # Execute remaining batch
    if batch:
        session.run("""
            UNWIND $batch AS record
            MERGE (p:Patient {patient_id: record.pid})
            MERGE (study:Study {study_id: record.study_id})
            MERGE (p)-[:HAS_STUDY]->(study)
            MERGE (series:Series {series_id: record.series_id})
            MERGE (study)-[:HAS_SERIES]->(series)
            CREATE (img:Image {
                file_name: record.file_name,
                file_path: record.file_path,
                slice_thickness: record.slice_thickness,
                pixel_spacing_x: record.px_x,
                pixel_spacing_y: record.px_y,
                manufacturer: record.manufacturer,
                modality: record.modality
            })
            CREATE (series)-[:CONTAINS]->(img)
        """, batch=batch)
    
    t_insert = time.time() - start_time

    # ----------------- Use Case A: Simple Lookups (Redis Strength) -----------------
    # A1: Get single image by file_name
    start_time = time.time()
    result_a1 = session.run("""
        MATCH (img:Image {file_name: $file_name})
        RETURN img
    """, file_name=sample_file_name)
    res_a1 = [record["img"] for record in result_a1]
    t_query_a1 = time.time() - start_time

    # A2: Get all images for a patient
    start_time = time.time()
    result_a2 = session.run("""
        MATCH (p:Patient {patient_id: $pid})-[:HAS_STUDY]->(:Study)-[:HAS_SERIES]->(:Series)-[:CONTAINS]->(img:Image)
        RETURN img
    """, pid=sample_patient_id)
    res_a2 = [record["img"] for record in result_a2]
    t_query_a2 = time.time() - start_time

    # ----------------- Use Case B: Multi-Hop Relationship Queries (Neo4j Strength) -----------------
    # B: Find patients connected through shared Studies or Series (true multi-hop traversal)
    # This demonstrates Neo4j's strength: traversing multiple relationship hops efficiently
    # Path: Patient1 -> Study -> Series -> (shared Study/Series) <- Series <- Study <- Patient2
    start_time = time.time()
    result_b = session.run("""
        // Multi-hop traversal: Find patients connected through shared Studies (2 hops: Patient->Study->Patient)
        MATCH (p1:Patient {patient_id: $pid})-[:HAS_STUDY]->(shared_study:Study)<-[:HAS_STUDY]-(p2:Patient)
        WHERE p1 <> p2
        RETURN DISTINCT p2.patient_id as patient_id
        UNION
        // Also find patients connected through shared Series (4 hops: Patient->Study->Series->Study->Patient)
        MATCH (p1:Patient {patient_id: $pid})-[:HAS_STUDY]->(:Study)-[:HAS_SERIES]->(shared_series:Series)<-[:HAS_SERIES]-(:Study)<-[:HAS_STUDY]-(p2:Patient)
        WHERE p1 <> p2
        RETURN DISTINCT p2.patient_id as patient_id
    """, pid=sample_patient_id)
    res_b = [record["patient_id"] for record in result_b]
    t_query_b = time.time() - start_time

    # ----------------- Use Case C: Complex Multi-Level Aggregation & Grouping (MongoDB Strength) -----------------
    # C: Multi-level grouping by manufacturer AND slice_thickness with complex statistics
    # Neo4j can do this but MongoDB's aggregation pipeline is more powerful for nested aggregations
    start_time = time.time()
    result_c = session.run("""
        // Start with Image nodes and traverse back to Patient
        MATCH (img:Image)<-[:CONTAINS]-(series:Series)<-[:HAS_SERIES]-(study:Study)<-[:HAS_STUDY]-(patient:Patient)
        
        // First level grouping: manufacturer + slice_thickness
        WITH img.manufacturer as manufacturer, 
            img.slice_thickness as slice_thickness,
            img.pixel_spacing_x as px_x,
            patient.patient_id as patient_id,
            study.study_id as study_id,
            series.series_id as series_id
            
        WITH manufacturer, slice_thickness,
            count(*) as image_count,
            avg(px_x) as avg_pixel_spacing_x,
            min(px_x) as min_pixel_spacing_x,
            max(px_x) as max_pixel_spacing_x,
            stDev(px_x) as stddev_pixel_spacing_x,
            collect(DISTINCT patient_id) as patients,
            collect(DISTINCT study_id) as studies,
            collect(DISTINCT series_id) as series
            
        WITH manufacturer, slice_thickness, image_count,
            avg_pixel_spacing_x, min_pixel_spacing_x, max_pixel_spacing_x, stddev_pixel_spacing_x,
            size(patients) as patient_count,
            size(studies) as study_count,
            size(series) as series_count,
            CASE WHEN size(patients) > 0 
                THEN toFloat(image_count) / size(patients) 
                ELSE 0.0 
            END as avg_images_per_patient
            
        // Second level: Group by manufacturer
        WITH manufacturer,
            sum(image_count) as total_images,
            sum(patient_count) as total_patients,
            collect({
                slice_thickness: slice_thickness,
                image_count: image_count,
                avg_pixel_spacing_x: avg_pixel_spacing_x,
                stddev_slice_thickness: 0.0,
                patient_count: patient_count
            }) as slice_thickness_groups,
            avg(avg_pixel_spacing_x) as overall_avg_pixel_spacing_x,
            min(slice_thickness) as min_slice_thickness,
            max(slice_thickness) as max_slice_thickness
            
        RETURN manufacturer, total_images, total_patients, slice_thickness_groups,
            overall_avg_pixel_spacing_x, min_slice_thickness, max_slice_thickness
        ORDER BY total_images DESC
    """)
    res_c = [dict(record) for record in result_c]
    t_query_c = time.time() - start_time

    # ----------------- Use Case D: Complex Multi-Condition Queries -----------------
    # D: Filter by manufacturer AND slice_thickness AND pixel_spacing range
    start_time = time.time()
    result_d = session.run("""
        MATCH (img:Image)
        WHERE img.manufacturer = 'SIEMENS'
          AND img.slice_thickness < 5.0
          AND img.pixel_spacing_x >= 0.6
          AND img.pixel_spacing_x <= 0.7
        RETURN img.file_name, img.manufacturer, img.slice_thickness, img.pixel_spacing_x
    """)
    res_d = [dict(record) for record in result_d]
    t_query_d = time.time() - start_time

results.append({
    "DB": "Neo4j",
    "InsertTime": t_insert,
    "UseCaseA_SimpleLookup_Time": t_query_a1 + t_query_a2,
    "UseCaseB_Relationship_Time": t_query_b,
    "UseCaseC_Aggregation_Time": t_query_c,
    "UseCaseD_ComplexFilter_Time": t_query_d,
    "Records": len(data_list),
    "UseCaseA_Records": len(res_a2),
    "UseCaseB_Records": len(res_b) if isinstance(res_b, (list, set)) else 1,
    "UseCaseC_Groups": len(res_c),
    "UseCaseD_Records": len(res_d)
})
print(f"Neo4j: Insert {t_insert:.2f}s | A:SimpleLookup {t_query_a1+t_query_a2:.3f}s | B:Relationship {t_query_b:.3f}s | C:Aggregation {t_query_c:.3f}s | D:ComplexFilter {t_query_d:.3f}s")
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
        # Get file_names for patient
        file_names = r.smembers(f"patient:{pid}:images") or set()
        # Fetch all images using pipeline
        pipe = r.pipeline()
        for file_name in file_names:
            pipe.hgetall(f"image:{file_name}")
        records = pipe.execute()
        times.append(time.time() - start)
    return times

def benchmark_sequential_patient_query_neo4j(session, patient_ids):
    times = []
    for pid in patient_ids:
        start = time.time()
        result = session.run("""
            MATCH (p:Patient)-[:HAS_STUDY]->(:Study)-[:HAS_SERIES]->(:Series)-[:CONTAINS]->(img:Image)
            WHERE p.patient_id = $pid
            RETURN img
        """, pid=pid)
        _ = [record["img"] for record in result]
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
