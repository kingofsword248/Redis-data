import requests, json, time, random, psutil, os, pandas as pd
from pymongo import MongoClient
import gridfs
import redis
from neo4j import GraphDatabase
import copy
import statistics
import pydicom
from PIL import Image
import numpy as np
import io
import base64



# ----------------- 0. Download data -----------------
# url = "https://spine-mri-public-data.s3.ap-southeast-1.amazonaws.com/transformed/cleaned_mri_data.json"
# print("Downloading data from S3...")
# data_list = requests.get(url).json()
# print(f"Downloaded {len(data_list)} records ✅")
import os
import json
from pprint import pprint # For nicely formatted printing

# Define the absolute path to your folder
folder_path = "cleaned_mri_data.json"


data_list = []
with open(folder_path, 'r') as file:
    data_list = json.load(file)
data_list = [
    {**item, "notes": item.get("annotations", [{}])[0].get("notes", "")}
    for item in data_list
]

def raw_img_to_base64(full_file_path):
    """
    Convert a .ima (DICOM) file to PNG format.
    
    Args:
        full_file_path: Full path to the .ima file
        
    Returns:
        Path to the created PNG file, or None if conversion fails
    """
    try:
        # Read the DICOM file
        dicom_data = pydicom.dcmread(full_file_path)
        
        # Extract pixel array
        pixel_array = dicom_data.pixel_array
        
        # Normalize pixel values to 0-255 range
        # Handle different bit depths and data types
        if pixel_array.dtype != np.uint8:
            # Normalize to 0-255 range
            pixel_min = pixel_array.min()
            pixel_max = pixel_array.max()
            
            if pixel_max > pixel_min:
                # Normalize: (pixel - min) / (max - min) * 255
                normalized = ((pixel_array.astype(np.float32) - pixel_min) / 
                             (pixel_max - pixel_min) * 255).astype(np.uint8)
            else:
                # All pixels have the same value
                normalized = np.zeros_like(pixel_array, dtype=np.uint8)
        else:
            normalized = pixel_array
        
        # Create PIL Image from numpy array
        # Handle grayscale (2D) and color (3D) images
        if len(normalized.shape) == 2:
            # Grayscale image
            image = Image.fromarray(normalized, mode='L')
        elif len(normalized.shape) == 3:
            # Color image (RGB)
            image = Image.fromarray(normalized, mode='RGB')
        else:
            print(f"Unsupported image shape: {normalized.shape} for {full_file_path}")
            return None
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='PNG')
        base64_img = img_byte_arr.getvalue()
        # Encode to base64 string since Redis has decode_responses=True
        base64_img = base64.b64encode(base64_img).decode('utf-8')
        return base64_img
        
    except Exception as e:
        print(f"Error converting {full_file_path} to PNG: {str(e)}")
        return None
# ETL for image 
# 1. load images from local folder  
folder_path = '../lumbar-mri-storage/src/dataset/01_MRI_Data'
for data in data_list:
    file_path = data['file_path']
    full_file_path = os.path.join(folder_path, file_path)
    if os.path.exists(full_file_path):
        print(f"Image found: {full_file_path}") 
        # convert .ima file to png file 
        image = raw_img_to_base64(full_file_path)
        # print(image)
        # implement details download this file
        # save this image to local folder
        #image.save(f"binhtest.png")
    else:
        print(f"Image not found: {full_file_path}")
    break
def allow_upload_image(patient_id):
    if(int(patient_id) <= 30):
        return True
    else:
        return False
##### mongo 

data_for_mongo = copy.deepcopy(data_list)
results_benchmark = []

# ----------------- Chuẩn bị 100 patient_id random duy nhất cho cả 3 DB -----------------
all_patient_ids = list(set(d['patient_id'] for d in data_list))
sample_ids = random.sample(all_patient_ids, min(100, len(all_patient_ids)))
print(f"Using the same {len(sample_ids)} random patient_id for all DBs ✅")

# ----------------- 1. MongoDB -----------------
print("\n=== MongoDB Benchmark ===")
mongo_client = MongoClient("mongodb://localhost:27000")
db = mongo_client["mri"]
mdb = db["metadata"]
mdb.drop()  # xóa collection cũ
db["image_storage.files"].drop()
db["image_storage.chunks"].drop()
fs = gridfs.GridFS(db, "image_storage")



# Insert tất cả scan vào 1 collection
start_time = time.time()

# Tạo indexes để query nhanh
mdb.create_index("patient_id")
mdb.create_index("file_name")
mdb.create_index("manufacturer")
mdb.create_index("slice_thickness")

# Upload images to GridFS
for data in data_for_mongo:
    data['image_gridfs_id'] = None
    if(allow_upload_image(data['patient_id'])):
        file_path = data['file_path']
        full_file_path = os.path.join(folder_path, file_path)
        with open(full_file_path, 'rb') as f:
            gridfs_id = fs.put(f, file_name=data['file_name'], study_id=data['study_id'], series_id=data['series_id'], patient_id=data['patient_id'])
            data['image_gridfs_id'] = gridfs_id
    
mdb.insert_many(data_for_mongo)
t_insert = time.time() - start_time






# ----------------- Use Case A: Simple Lookups (Redis Strength) -----------------


# A2: Get all images for a patient
sample_patient_id = "0020"
print(f"Sample patient ID: {sample_patient_id}")
start_time = time.time()
a_2 = mdb.find({"patient_id": sample_patient_id})
res_a2 = list(a_2)
t_query_a2 = time.time() - start_time

# print(res_a2)
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
        "slice_thickness_groups": {"$push": {
            "slice_thickness": "$_id.slice_thickness",
            "image_count": "$image_count",
            "avg_pixel_spacing_x": "$avg_pixel_spacing_x",
            "stddev_slice_thickness": "$stddev_slice_thickness",
            
        }},
        "overall_avg_pixel_spacing_x": {"$avg": "$avg_pixel_spacing_x"},
        "min_slice_thickness": {"$min": "$_id.slice_thickness"},
        "max_slice_thickness": {"$max": "$_id.slice_thickness"}
    }},
    {"$sort": {"total_images": -1}}
]))
t_query_c = time.time() - start_time
# print(res_c)
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

# ----------------- Use Case E: Update Notes for Patient -----------------
# E: Update notes for sample_patient_id
new_notes = "Updated notes: Patient follow-up scheduled for next month."
start_time = time.time()
res_e = mdb.update_many(
    {"patient_id": sample_patient_id},
    {"$set": {"notes": new_notes}}
)
t_query_e = time.time() - start_time

# ----------------- Use Case F: Delete Patient -----------------
# F: Delete sample_patient_id and all associated records
start_time = time.time()
res_f = mdb.delete_many({"patient_id": sample_patient_id})
t_query_f = time.time() - start_time

# ----------------- Use Case G: Add New Patient -----------------
# G: Add a new patient with patient_id = "binhtest"
new_patient_id = "binhtest"
# Create sample image data for the new patient
sample_image_data = {
    "patient_id": new_patient_id,
    "study_id": f"study_{new_patient_id}_001",
    "series_id": f"series_{new_patient_id}_001",
    "file_name": f"image_{new_patient_id}_001.dcm",
    "file_path": f"/data/{new_patient_id}/image_001.dcm",
    "slice_thickness": 3.0,
    "pixel_spacing": [0.625, 0.625],
    "manufacturer": "SIEMENS",
    "modality": "MR",
    "notes": "New test patient added via benchmark"
}
start_time = time.time()
res_g = mdb.insert_one(sample_image_data)
t_query_g = time.time() - start_time

results_benchmark.append({
    "DB": "MongoDB",
    "InsertTime": t_insert,
    "UseCaseA_SimpleLookup_Time": t_query_a2,
    "UseCaseB_Relationship_Time": t_query_b,
    "UseCaseC_Aggregation_Time": t_query_c,
    "UseCaseD_ComplexFilter_Time": t_query_d,
    "UseCaseE_UpdateNotes_Time": t_query_e,
    "UseCaseF_DeletePatient_Time": t_query_f,
    "UseCaseG_AddPatient_Time": t_query_g,
    "Records": len(data_list),
    "UseCaseA_Records": len(res_a2) if res_a2 else 0,
    "UseCaseB_Records": len(res_b) if isinstance(res_b, (list, set)) else 1,
    "UseCaseC_Groups": len(res_c),
    "UseCaseD_Records": len(res_d),
    "UseCaseE_UpdatedRecords": res_e.modified_count if res_e else 0,
    "UseCaseF_DeletedRecords": res_f.deleted_count if res_f else 0,
    "UseCaseG_InsertedRecords": 1 if res_g.inserted_id else 0
})
print(f"MongoDB: Insert {t_insert:.2f}s | A:SimpleLookup {t_query_a2:.3f}s | B:Relationship {t_query_b:.3f}s | C:Aggregation {t_query_c:.3f}s | D:ComplexFilter {t_query_d:.3f}s | E:UpdateNotes {t_query_e:.3f}s | F:DeletePatient {t_query_f:.3f}s | G:AddPatient {t_query_g:.5f}s | UseCaseA_Records: {len(res_a2)} | UseCaseB_Records: {len(res_b)} | UseCaseC_Groups: {len(res_c)} | UseCaseD_Records: {len(res_d)} | UseCaseE_UpdatedRecords: {res_e.modified_count if res_e else 0} | UseCaseF_DeletedRecords: {res_f.deleted_count if res_f else 0} | UseCaseG_InsertedRecords: {1 if res_g.inserted_id else 0}")

#### ETL image for redis + neo4j 
data_list_with_base64 = copy.deepcopy(data_list)
for d in data_list_with_base64:
    if(allow_upload_image(d['patient_id'])):
        file_path = d['file_path']
        full_file_path = os.path.join(folder_path, file_path)
        if os.path.exists(full_file_path):
            base64_img = raw_img_to_base64(full_file_path)
            d['base64_img'] = base64_img
        else:
            print(f"Image not found: {full_file_path}")
print(len(data_list_with_base64))

# ----------------- 2. Redis Benchmark (Optimized) -----------------
print("\n=== Redis Benchmark ===")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
r.flushdb()

start_time = time.time()
# Use pipeline for batch operations
pipe = r.pipeline()
pipeline_count = 0

for d in data_list_with_base64:
    file_path = d["file_path"]
    pid = d["patient_id"]
    
    # Handle pixel_spacing
    pixel_spacing = d["pixel_spacing"]
    px_x = pixel_spacing[0] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 0 else pixel_spacing
    px_y = pixel_spacing[1] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 1 else pixel_spacing
   
    # Store image as hash
    pipe.hset(f"image:{file_path}", mapping={
        "patient_id": pid,
        "study_id": d.get("study_id", ""),
        "series_id": d["series_id"],
        "slice_thickness": str(d["slice_thickness"]),
        "pixel_spacing_x": str(px_x),
        "pixel_spacing_y": str(px_y),
        "manufacturer": d["manufacturer"],
        "modality": d["modality"],
        "file_path": d["file_path"],
        "base64_img": d.get("base64_img", "")
    })
    
    # Add to patient index
    pipe.sadd(f"patient:{pid}:images", file_path)
    pipe.set(f"patient:{pid}:notes", d["notes"])
    # Add to manufacturer index
    if(file_path not in r.smembers(f"manufacturer:{d['manufacturer']}:images")):
        pipe.sadd(f"manufacturer:{d['manufacturer']}:images", file_path)
    
    # Add to slice_thickness index
    if(file_path not in r.smembers(f"slice_thickness:{d['slice_thickness']}")):
        pipe.sadd(f"slice_thickness:{d['slice_thickness']}", file_path)

pipe.execute()

t_insert = time.time() - start_time
print(f"Redis insert time: {t_insert:.2f}s")


# ----------------- Use Case A: Simple Lookups (Redis Strength) -----------------


# A2: Get all images for a patient (Set lookup + Hash retrieval)
start_time = time.time()
file_names = r.smembers(f"patient:{sample_patient_id}:images") or set()

res_a2 = list(file_names)
t_query_a2 = time.time() - start_time

# print(res_a2)
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
    for file_path in manufacturer_files:
        pipe.hmget(f"image:{file_path}", "slice_thickness", "pixel_spacing_x", "patient_id", "study_id", "series_id")
    all_images_data = pipe.execute()
    
    # Group by slice_thickness
    thickness_groups = {}
    for file_path, img_data in zip(manufacturer_files, all_images_data):
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
        
        thickness_groups[thickness]["images"].append(file_path)
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
    all_pixel_spacing = []
    
    for thickness, group_data in thickness_groups.items():
        image_count = len(group_data["images"])
        total_images += image_count
        
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
        "slice_thickness_groups": slice_thickness_groups,
        "overall_avg_pixel_spacing_x": overall_avg_px,
        "min_slice_thickness": min(thickness_groups.keys()) if thickness_groups else 0,
        "max_slice_thickness": max(thickness_groups.keys()) if thickness_groups else 0
    })

res_c.sort(key=lambda x: x["total_images"], reverse=True)
t_query_c = time.time() - start_time
# print(res_c)
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

# ----------------- Use Case E: Update Notes for Patient -----------------
# E: Update notes for sample_patient_id
new_notes = "Updated notes: Patient follow-up scheduled for next month."
start_time = time.time()
res_e = r.set(f"patient:{sample_patient_id}:notes", new_notes)
t_query_e = time.time() - start_time

# ----------------- Use Case F: Delete Patient -----------------
# F: Delete sample_patient_id and all associated records
start_time = time.time()
# Get all image file paths for this patient
patient_images = r.smembers(f"patient:{sample_patient_id}:images") or set()
deleted_count = 0

# Delete all image hashes
pipe = r.pipeline()
for file_path in patient_images:
    pipe.delete(f"image:{file_path}")
    deleted_count += 1

# Delete patient-related keys
pipe.delete(f"patient:{sample_patient_id}:images")
pipe.delete(f"patient:{sample_patient_id}:notes")
pipe.execute()
t_query_f = time.time() - start_time

# ----------------- Use Case G: Add New Patient -----------------
# G: Add a new patient with patient_id = "binhtest"
new_patient_id = "binhtest"
file_path = f"/data/{new_patient_id}/image_001.dcm"

pipe = r.pipeline()
start_time = time.time()
# Store image as hash
pipe.hset(f"image:{file_path}", mapping={
    "patient_id": new_patient_id,
    "study_id": f"study_{new_patient_id}_001",
    "series_id": f"series_{new_patient_id}_001",
    "slice_thickness": "3.0",
    "pixel_spacing_x": "0.625",
    "pixel_spacing_y": "0.625",
    "manufacturer": "SIEMENS",
    "modality": "MR",
    "file_path": file_path
})
pipe.execute()
t_query_g = time.time() - start_time
pipe = r.pipeline()
# Add to patient index
pipe.sadd(f"patient:{new_patient_id}:images", file_path)

pipe.set(f"patient:{new_patient_id}:notes", "New test patient added via benchmark")
# Add to manufacturer index
pipe.sadd(f"manufacturer:SIEMENS:images", file_path)
# Add to slice_thickness index
pipe.sadd(f"slice_thickness:3.0", file_path)
pipe.execute()

results_benchmark.append({
    "DB": "Redis",
    "InsertTime": t_insert,
    "UseCaseA_SimpleLookup_Time": t_query_a2,
    "UseCaseB_Relationship_Time": t_query_b,
    "UseCaseC_Aggregation_Time": t_query_c,
    "UseCaseD_ComplexFilter_Time": t_query_d,
    "UseCaseE_UpdateNotes_Time": t_query_e,
    "UseCaseF_DeletePatient_Time": t_query_f,
    "UseCaseG_AddPatient_Time": t_query_g,
    "Records": len(data_list),
    "UseCaseA_Records": len(res_a2),
    "UseCaseB_Records": len(res_b) if isinstance(res_b, (list, set)) else 1,
    "UseCaseC_Groups": len(res_c),
    "UseCaseD_Records": len(res_d),
    "UseCaseE_UpdatedRecords": 1 if res_e else 0,
    "UseCaseF_DeletedRecords": deleted_count,
    "UseCaseG_InsertedRecords": 1
})
print(f"Redis: Insert {t_insert:.2f}s | A:SimpleLookup {t_query_a2:.3f}s | B:Relationship {t_query_b:.3f}s | C:Aggregation {t_query_c:.3f}s | D:ComplexFilter {t_query_d:.3f}s | E:UpdateNotes {t_query_e:.3f}s | F:DeletePatient {t_query_f:.3f}s | G:AddPatient {t_query_g:.5f}s | UseCaseA_Records: {len(res_a2)} | UseCaseB_Records: {len(res_b)} | UseCaseC_Groups: {len(res_c)} | UseCaseD_Records: {len(res_d)} | UseCaseE_UpdatedRecords: {1 if res_e else 0} | UseCaseF_DeletedRecords: {deleted_count} | UseCaseG_InsertedRecords: 1")

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
    batch = []
    for d in data_list_with_base64:
        # Handle pixel_spacing as array
        pixel_spacing = d["pixel_spacing"]
        px_x = pixel_spacing[0] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 0 else pixel_spacing
        px_y = pixel_spacing[1] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 1 else pixel_spacing
        
   
        data = {
            "pid": d["patient_id"],
            "study_id": d.get("study_id", d["series_id"]),  # Use series_id as fallback if study_id doesn't exist
            "series_id": d["series_id"],
            "file_name": d["file_name"],
            "file_path": d["file_path"],
            "slice_thickness": d["slice_thickness"],
            "px_x": px_x,
            "px_y": px_y,
            "manufacturer": d["manufacturer"],
            "modality": d["modality"],
            "notes": d["notes"],
           
        }
        if(d.get('base64_img')):
            data['base64_img'] = d.get('base64_img')
        batch.append(data)
    
    # Execute batch
    if batch:
        session.run("""
            UNWIND $batch AS record
            MERGE (p:Patient {patient_id: record.pid})
            
            // Create a single Notes node per patient
            CREATE (n:Notes {content: record.notes, created_at: timestamp()})
            MERGE (p)-[:HAS_NOTES]->(n)
            
            MERGE (study:Study {study_id: record.study_id})
            MERGE (p)-[:HAS_STUDY]->(study)
            MERGE (series:Series {series_id: record.series_id})
            MERGE (study)-[:HAS_SERIES]->(series)
            CREATE (img:Image {
                base64_img: record.base64_img,
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
    print(f"Neo4j: Insert {t_insert:.2f}s")

    # ----------------- Use Case A: Simple Lookups (Redis Strength) -----------------
   

    # A2: Get all images for a patient
    start_time = time.time()
    result_a2 = session.run("""
        MATCH (p:Patient {patient_id: $pid})-[:HAS_STUDY]->(:Study)-[:HAS_SERIES]->(:Series)-[:CONTAINS]->(img:Image)
        RETURN img
    """, pid=sample_patient_id)
    res_a2 = [record["img"] for record in result_a2]
    t_query_a2 = time.time() - start_time
  
    # print(res_a2[0])
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
        MATCH (img:Image)<-[:CONTAINS]-(series:Series)<-[:HAS_SERIES]-(study:Study)<-[:HAS_STUDY]-(patient:Patient)
        WITH img.manufacturer AS manufacturer, 
            img.slice_thickness AS slice_thickness,
            img.pixel_spacing_x AS px_x,
            patient.patient_id AS patient_id,
            study.study_id AS study_id,
            series.series_id AS series_id

        // Group by manufacturer + slice_thickness
        WITH manufacturer, slice_thickness,
            count(*) AS image_count,
            avg(px_x) AS avg_pixel_spacing_x,
            collect(DISTINCT patient_id) AS patients_list,
            collect(DISTINCT study_id) AS studies_list,
            collect(DISTINCT series_id) AS series_list

        // Build slice_thickness_groups
        WITH manufacturer,
            collect({
                slice_thickness: slice_thickness,
                image_count: image_count,
                avg_pixel_spacing_x: avg_pixel_spacing_x,
                stddev_slice_thickness: 0.0,
                patient_count: size(patients_list)
            }) AS slice_thickness_groups,
            collect(patients_list) AS grouped_patients,
            sum(image_count) AS total_images,
            avg(avg_pixel_spacing_x) AS overall_avg_pixel_spacing_x,
            min(slice_thickness) AS min_slice_thickness,
            max(slice_thickness) AS max_slice_thickness

        // Flatten + remove duplicates using UNWIND (pure Cypher, no APOC needed)
        UNWIND grouped_patients AS patient_list
        UNWIND patient_list AS patient_id
        WITH manufacturer,
            slice_thickness_groups,
            total_images,
            overall_avg_pixel_spacing_x,
            min_slice_thickness,
            max_slice_thickness,
            collect(DISTINCT patient_id) AS all_patients

        // Final Output
        RETURN manufacturer,
            total_images,
            slice_thickness_groups,
            overall_avg_pixel_spacing_x,
            min_slice_thickness,
            max_slice_thickness
        ORDER BY total_images DESC;

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

    # ----------------- Use Case E: Update Notes for Patient -----------------
    # E: Update notes for sample_patient_id
    new_notes = "Updated notes: Patient follow-up scheduled for next month."
    start_time = time.time()
    result_e = session.run("""
        MATCH (p:Patient {patient_id: $pid})-[r:HAS_NOTES]->(old_notes:Notes)
        // Create a new Notes node with updated content
        CREATE (n:Notes {content: $new_notes, created_at: timestamp()})
        // Connect patient to new notes
        CREATE (p)-[:HAS_NOTES]->(n)
        // Delete the old relationship and notes node
        DELETE r, old_notes
        RETURN n
    """, pid=sample_patient_id, new_notes=new_notes)
    res_e = [record["n"] for record in result_e]
    t_query_e = time.time() - start_time

    # ----------------- Use Case F: Delete Patient -----------------
    # F: Delete sample_patient_id and all associated nodes/relationships
    start_time = time.time()
    # First, count images to be deleted
    count_result = session.run("""
        MATCH (p:Patient {patient_id: $pid})-[:HAS_STUDY]->(:Study)-[:HAS_SERIES]->(:Series)-[:CONTAINS]->(img:Image)
        RETURN count(DISTINCT img) as img_count
    """, pid=sample_patient_id)
    img_count = [record["img_count"] for record in count_result]
    deleted_images_count = img_count[0] if img_count else 0
    
    # Delete patient and all connected nodes (cascading delete)
    session.run("""
        MATCH (p:Patient {patient_id: $pid})
        OPTIONAL MATCH (p)-[:HAS_NOTES]->(notes:Notes)
        OPTIONAL MATCH (p)-[:HAS_STUDY]->(study:Study)
        OPTIONAL MATCH (study)-[:HAS_SERIES]->(series:Series)
        OPTIONAL MATCH (series)-[:CONTAINS]->(img:Image)
        DETACH DELETE p, notes, study, series, img
    """, pid=sample_patient_id)
    t_query_f = time.time() - start_time
    deleted_count_f = deleted_images_count

    # ----------------- Use Case G: Add New Patient -----------------
    # G: Add a new patient with patient_id = "binhtest"
    new_patient_id = "binhtest"
    start_time = time.time()
    result_g = session.run("""
        MERGE (p:Patient {patient_id: $pid})
        
        // Create Notes node
        CREATE (n:Notes {content: $notes, created_at: timestamp()})
        CREATE (p)-[:HAS_NOTES]->(n)
        
        MERGE (study:Study {study_id: $study_id})
        MERGE (p)-[:HAS_STUDY]->(study)
        MERGE (series:Series {series_id: $series_id})
        MERGE (study)-[:HAS_SERIES]->(series)
        CREATE (img:Image {
            file_name: $file_name,
            file_path: $file_path,
            slice_thickness: $slice_thickness,
            pixel_spacing_x: $px_x,
            pixel_spacing_y: $px_y,
            manufacturer: $manufacturer,
            modality: $modality
        })
        CREATE (series)-[:CONTAINS]->(img)
        RETURN p, n, img
    """, 
        pid=new_patient_id,
        notes="New test patient added via benchmark",
        study_id=f"study_{new_patient_id}_001",
        series_id=f"series_{new_patient_id}_001",
        file_name=f"image_{new_patient_id}_001.dcm",
        file_path=f"/data/{new_patient_id}/image_001.dcm",
        slice_thickness=3.0,
        px_x=0.625,
        px_y=0.625,
        manufacturer="SIEMENS",
        modality="MR"
    )
    res_g = [record for record in result_g]
    t_query_g = time.time() - start_time

results_benchmark.append({
    "DB": "Neo4j",
    "InsertTime": t_insert,
    "UseCaseA_SimpleLookup_Time": t_query_a2,
    "UseCaseB_Relationship_Time": t_query_b,
    "UseCaseC_Aggregation_Time": t_query_c,
    "UseCaseD_ComplexFilter_Time": t_query_d,
    "UseCaseE_UpdateNotes_Time": t_query_e,
    "UseCaseF_DeletePatient_Time": t_query_f,
    "UseCaseG_AddPatient_Time": t_query_g,
    "Records": len(data_list),
    "UseCaseA_Records": len(res_a2),
    "UseCaseB_Records": len(res_b) if isinstance(res_b, (list, set)) else 1,
    "UseCaseC_Groups": len(res_c),
    "UseCaseD_Records": len(res_d),
    "UseCaseE_UpdatedRecords": len(res_e),
    "UseCaseF_DeletedRecords": deleted_count_f,
    "UseCaseG_InsertedRecords": len(res_g)
})
# print(res_c)
print(f"Neo4j: Insert {t_insert:.2f}s | A:SimpleLookup {t_query_a2:.3f}s | B:Relationship {t_query_b:.3f}s | C:Aggregation {t_query_c:.3f}s | D:ComplexFilter {t_query_d:.3f}s | E:UpdateNotes {t_query_e:.3f}s | F:DeletePatient {t_query_f:.3f}s | G:AddPatient {t_query_g:.5f}s | UseCaseA_Records: {len(res_a2)} | UseCaseB_Records: {len(res_b)} | UseCaseC_Groups: {len(res_c)} | UseCaseD_Records: {len(res_d)} | UseCaseE_UpdatedRecords: {len(res_e)} | UseCaseF_DeletedRecords: {deleted_count_f} | UseCaseG_InsertedRecords: {len(res_g)}")
print("\n===========================================")

# ----------------- 4. Xuất CSV -----------------
df = pd.DataFrame(results_benchmark)
df.to_csv("benchmark.csv", index=False)
print("\n✅ Benchmark completed. Results saved to benchmark.csv")
print(df)
