import requests, json, time, random, psutil, os, pandas as pd, redis
from neo4j import GraphDatabase
import copy
import statistics
import base64
import io
from PIL import Image
import pydicom
import numpy as np
from pathlib import Path
from redis.client import Redis, StrictRedis

# Function to convert image to bytes - handling DICOM files
def image_to_bytes(image_path):
    try:
        file_extension = image_path.suffix.lower()
        # Handle DICOM files (.ima, .dcm)
        if file_extension in ['.ima', '.dcm']:
            try:
                dicom_data = pydicom.dcmread(image_path)
                # Convert pixel data to PNG format
                if hasattr(dicom_data, 'pixel_array'):
                    # Normalize pixel values to 0-255 range
                    pixel_array = dicom_data.pixel_array
                    if pixel_array.max() > 0:  # Avoid division by zero
                        pixel_array = (pixel_array / pixel_array.max() * 255).astype(np.uint8)
                    # Convert to PIL Image
                    img = Image.fromarray(pixel_array)
                    # Resize image to about 0-255px
                    img.thumbnail((255, 255))
                    # Save to buffer
                    buffer = io.BytesIO()
                    img.save(buffer, format='PNG')
                    return base64.b64encode(buffer.getvalue()).decode('utf-8')
                else:
                    # If no pixel data, store the raw DICOM data
                    buffer = io.BytesIO()
                    dicom_data.save_as(buffer)
                    return base64.b64encode(buffer.getvalue()).decode('utf-8')
            except Exception as e:
                print(f"Error processing DICOM file {image_path}: {e}")
                return None
        # Handle regular image files
        else:
            with Image.open(image_path) as img:
                # Resize image to about 0-255px
                img.thumbnail((255, 255))
                buffer = io.BytesIO()
                img.save(buffer, format=img.format if img.format else 'PNG')
                return base64.b64encode(buffer.getvalue()).decode('utf-8')
    except Exception as e:
        print(f"Error converting image {image_path}: {e}")
        return None

# ----------------- 0. Download data -----------------
url = "https://spine-mri-public-data.s3.ap-southeast-1.amazonaws.com/transformed/cleaned_mri_data.json"
print("Downloading data from S3...")
data_list = requests.get(url).json()
print(f"Downloaded {len(data_list)} records ✅")
results = []

# Define your local root directory where images are stored
# Replace this with your actual local path
LOCAL_ROOT_DIR = r"01_MRI_Data"

# ----------------- Prepare sample patient IDs -----------------
# Filter for patient IDs 0001 to 0030
target_patients = [f"{i:04d}" for i in range(1, 31)]
print(f"Using patient IDs from 0001 to 0030 for base64 image conversion ✅")
sample_patient_id = target_patients[0]  # Use first patient from target list

# ----------------- 2. Redis Benchmark (Optimized) -----------------
print("\n=== Redis Benchmark ===")
r = redis.Redis(host="localhost", port=6379, decode_responses=True)
r.flushdb()


# Gom các record theo patient_id
temp_dict = {}
for d in data_list:
    pid = d["patient_id"]
    item = {
        "study_id": d["study_id"],
        "series_id": d["series_id"],
        "study_date": d["study_date"],
        "slice_thickness": d["slice_thickness"],
        "pixel_spacing": d["pixel_spacing"],
        "orientation": d["orientation"],
        "manufacturer": d["manufacturer"],
        "modality": d["modality"],
        "ima": "",
        "file_name": d["file_name"],
        "file_path": "",
        "annotations": d["file_name"]
    }
    temp_dict.setdefault(pid, []).append(item)

start_time = time.time()
# Lưu vào Redis: key = patient_id, value = list Redis
for pid, records in temp_dict.items():
    r.rpush(f"patient_id:{pid}", *records)
t_insert = time.time() - start_time

# ----------------- Use Case A: Simple Lookups (Redis Strength) -----------------

start_time = time.time()
res_a2 = r.lrange(f"patient_id:{sample_patient_id}", 0, -1)
t_query_a2 = time.time() - start_time

results.append({
    "DB": "Redis",
    "InsertTime": t_insert,
    "UseCaseA_SimpleQuery_Time": t_query_a2,
    "UseCaseB_Relationship_Time": "N/A",
    "UseCaseC_Aggregation_Time": "N/A",
    "UseCaseD_ComplexFilter_Time": "N/A",
    "Records": len(data_list),
    "UseCaseA_Records": len(res_a2) if res_a2 else 0,
    "UseCaseB_Records": "N/A",
    "UseCaseC_Groups": "N/A",
    "UseCaseD_Records": "N/A"
})
print(
    f"Redis: Insert {t_insert:.2f}s | A:SimpleQuery { t_query_a2:.6f}s ")

# ----------------- 3. Neo4j -----------------
print("\n=== Neo4j Benchmark ===")
driver = GraphDatabase.driver("bolt://127.0.0.1:7687", auth=("neo4j","12345678"))
with driver.session(database='binary-ima') as session:
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
    batch_size = 100  # Smaller batch size due to base64 images
    batch = []
    
    for d in data_list:
        # Handle pixel_spacing as array
        pixel_spacing = d["pixel_spacing"]
        px_x = pixel_spacing[0] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 0 else pixel_spacing
        px_y = pixel_spacing[1] if isinstance(pixel_spacing, list) and len(pixel_spacing) > 1 else pixel_spacing
        
        # Check if patient_id is in target range for base64 conversion
        patient_id = d["patient_id"]
        image_base64 = None
                    
        if patient_id in target_patients:
            # Get just the filename from the S3 path
            file_path = d["file_path"]
            file_name = os.path.basename(file_path)
            series_id = d["series_id"]
            
            # Try multiple possible locations for the file based on the observed directory structure
            possible_paths = [
                # Based on your directory structure:
                # Patient ID folder -> Series folder -> Image file
                Path(os.path.join(LOCAL_ROOT_DIR, patient_id, series_id, file_name)),
                
                # Patient ID folder -> Series name folder -> Image file
                # (in case series_id doesn't match folder name exactly)
                Path(os.path.join(LOCAL_ROOT_DIR, patient_id, f"LOCALIZER_{patient_id}", file_name)),
                Path(os.path.join(LOCAL_ROOT_DIR, patient_id, f"T1_TSE_SAG_{series_id}", file_name)),
                Path(os.path.join(LOCAL_ROOT_DIR, patient_id, f"T1_TSE_TRA_{series_id}", file_name)),
                Path(os.path.join(LOCAL_ROOT_DIR, patient_id, f"T2_TSE_SAG_{series_id}", file_name)),
                Path(os.path.join(LOCAL_ROOT_DIR, patient_id, f"T2_TSE_TRA_{series_id}", file_name)),
                Path(os.path.join(LOCAL_ROOT_DIR, patient_id, f"POSDISP_[4]_T2_TSE_TRA_{series_id}", file_name)),
                
                # Try with just patient ID folder and filename (in case files are directly in patient folder)
                Path(os.path.join(LOCAL_ROOT_DIR, patient_id, file_name)),
                
                # Try direct filename in root directory (fallback)
                Path(os.path.join(LOCAL_ROOT_DIR, file_name)),
            ]
            
            # Search for files recursively in the patient directory
            patient_dir = Path(os.path.join(LOCAL_ROOT_DIR, patient_id))
            if patient_dir.exists():
                # Recursively search for the file in the patient directory
                for root, dirs, files in os.walk(patient_dir):
                    if file_name in files:
                        possible_paths.insert(0, Path(os.path.join(root, file_name)))
                        break
            
            # Try each possible path
            local_path = None
            for path in possible_paths:
                if path.exists():
                    local_path = path
                    break
            
            # If file is found, convert to base64
            if local_path and local_path.exists():
                image_base64 = image_to_bytes(local_path)
                if image_base64:
                    print(f"Converted image for patient {patient_id}, file: {file_name}")
                else:
                    print(f"Failed to convert image for patient {patient_id}, file: {file_name}")
            else:
                print(f"Image file not found for patient {patient_id}, file: {file_name}")
                
                # Print first few directories in patient folder to help debug
                patient_dir = Path(os.path.join(LOCAL_ROOT_DIR, patient_id))
                if patient_dir.exists():
                    print(f"  Directories in {patient_id} folder:")
                    try:
                        for i, item in enumerate(os.listdir(patient_dir)):
                            if os.path.isdir(os.path.join(patient_dir, item)):
                                print(f"    {item}")
                            if i > 5:  # Limit to first 6 directories
                                print("    ...")
                                break
                    except Exception as e:
                        print(f"  Error listing directories: {e}")

        batch.append({
            "pid": patient_id,
            "study_id": d.get("study_id", d["series_id"]),  # Use series_id as fallback if study_id doesn't exist
            "series_id": d["series_id"],
            "file_name": d["file_name"],
            "file_path": d["file_path"],
            "slice_thickness": d["slice_thickness"],
            "px_x": px_x,
            "px_y": px_y,
            "manufacturer": d["manufacturer"],
            "modality": d["modality"],
            "image_base64": image_base64
        })
        
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
                modality: record.modality,
                image_base64: record.image_base64
            })
            CREATE (series)-[:CONTAINS]->(img)
        """, batch=batch)
    
    t_insert = time.time() - start_time
    print(f"Neo4j insert time: {t_insert:.2f}s")

    # ----------------- Use Case A: Simple Lookups -----------------
    # A1: Get single image by file_name
    sample_file_name = data_list[0]["file_name"]
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

    # ----------------- Use Case B: Multi-Hop Relationship Queries -----------------
    # B: Find patients connected through shared Studies or Series
    start_time = time.time()
    result_b = session.run("""
        // Multi-hop traversal: Find patients connected through shared Studies
        MATCH (p1:Patient {patient_id: $pid})-[:HAS_STUDY]->(shared_study:Study)<-[:HAS_STUDY]-(p2:Patient)
        WHERE p1 <> p2
        RETURN DISTINCT p2.patient_id as patient_id
        UNION
        // Also find patients connected through shared Series
        MATCH (p1:Patient {patient_id: $pid})-[:HAS_STUDY]->(:Study)-[:HAS_SERIES]->(shared_series:Series)<-[:HAS_SERIES]-(:Study)<-[:HAS_STUDY]-(p2:Patient)
        WHERE p1 <> p2
        RETURN DISTINCT p2.patient_id as patient_id
    """, pid=sample_patient_id)
    res_b = [record["patient_id"] for record in result_b]
    t_query_b = time.time() - start_time

    # ----------------- Use Case C: Complex Multi-Level Aggregation & Grouping -----------------
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

    # ----------------- Use Case E: Query Images with Base64 Data -----------------
    start_time = time.time()
    # First check if any images have base64 data
    check_result = session.run("""
        MATCH (img:Image)
        WHERE img.image_base64 IS NOT NULL
        RETURN count(img) as count
    """)
    record = check_result.single()
    base64_count = record["count"] if record is not None else 0

    if base64_count > 0:
        # If we have base64 images, run the original query
        result_e = session.run("""
            MATCH (p:Patient)-[:HAS_STUDY]->(:Study)-[:HAS_SERIES]->(:Series)-[:CONTAINS]->(img:Image)
            WHERE p.patient_id IN $target_patients AND img.image_base64 IS NOT NULL
            RETURN p.patient_id, img.file_name, img.image_base64
            LIMIT 10
        """, target_patients=target_patients)
        res_e = [dict(record) for record in result_e]
        print(f"Found {len(res_e)} images with base64 data")
    else:
        # If no base64 images found, check what properties are available
        print("No images with base64 data found. Checking available properties...")
        prop_result = session.run("""
            MATCH (img:Image) 
            RETURN keys(img) as properties
            LIMIT 1
        """)
        props = prop_result.single()["properties"] if prop_result.single() else []
        print(f"Available Image properties: {props}")
        
        # Run a modified query without the base64 filter
        result_e = session.run("""
            MATCH (p:Patient)-[:HAS_STUDY]->(:Study)-[:HAS_SERIES]->(:Series)-[:CONTAINS]->(img:Image)
            WHERE p.patient_id IN $target_patients
            RETURN p.patient_id, img.file_name
            LIMIT 10
        """, target_patients=target_patients)
        res_e = [dict(record) for record in result_e]
        print(f"Found {len(res_e)} images (without base64 filter)")

    t_query_e = time.time() - start_time

    # ----------------- Sequential Queries Benchmark -----------------
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

    # Run sequential queries benchmark for Neo4j
    neo4j_times = benchmark_sequential_patient_query_neo4j(session, target_patients[:10])  # Use first 10 patients
    print(f"Neo4j sequential queries (10): min={min(neo4j_times):.4f}s, max={max(neo4j_times):.4f}s, avg={statistics.mean(neo4j_times):.4f}s")

    # ----------------- Base64 Image Size Statistics -----------------
    # First check if any images have base64 data
    check_result = session.run("""
        MATCH (img:Image)
        WHERE img.image_base64 IS NOT NULL
        RETURN count(img) as count
    """)
    record = check_result.single()
    base64_count = record["count"] if record is not None else 0
    
    if base64_count > 0:
        # If we have base64 images, run the original statistics query
        result = session.run("""
            MATCH (img:Image)
            WHERE img.image_base64 IS NOT NULL
            RETURN 
                count(img) as total_images_with_base64,
                avg(size(img.image_base64)) as avg_base64_size,
                min(size(img.image_base64)) as min_base64_size,
                max(size(img.image_base64)) as max_base64_size
        """)
        stats = result.single()
        if stats:
            print(f"\nBase64 Image Statistics:")
            print(f"Total images with base64: {stats['total_images_with_base64']}")
            print(f"Average base64 size: {stats['avg_base64_size']:.2f} characters")
            print(f"Min base64 size: {stats['min_base64_size']} characters")
            print(f"Max base64 size: {stats['max_base64_size']} characters")
    else:
        print("\nNo images with base64 data found for statistics.")
        
        # Check if any images were inserted
        count_result = session.run("""
            MATCH (img:Image)
            RETURN count(img) as total_images
        """)
        total = count_result.single()["total_images"] if count_result.single() else 0
        print(f"Total images in database: {total}")
        
        # Check if the base64 conversion worked but wasn't stored properly
        print("\nChecking for issues with base64 conversion...")
        conversion_count = sum(1 for d in data_list 
                              if d["patient_id"] in target_patients)
        print(f"Total images that should have been converted: {conversion_count}")

results.append({
    "DB": "Neo4j",
    "InsertTime": t_insert,
    "UseCaseA_SimpleLookup_Time": t_query_a1 + t_query_a2,
    "UseCaseB_Relationship_Time": t_query_b,
    "UseCaseC_Aggregation_Time": t_query_c,
    "UseCaseD_ComplexFilter_Time": t_query_d,
    "UseCaseE_Base64Query_Time": t_query_e,
    "Records": len(data_list),
    "UseCaseA_Records": len(res_a2),
    "UseCaseB_Records": len(res_b) if isinstance(res_b, (list, set)) else 1,
    "UseCaseC_Groups": len(res_c),
    "UseCaseD_Records": len(res_d),
    "UseCaseE_Records": len(res_e)
})
print(f"Neo4j: Insert {t_insert:.2f}s | A:SimpleLookup {t_query_a1+t_query_a2:.3f}s | B:Relationship {t_query_b:.3f}s | C:Aggregation {t_query_c:.3f}s | D:ComplexFilter {t_query_d:.3f}s | E:Base64Query {t_query_e:.3f}s")
print("\n===========================================")

# ----------------- 4. Export CSV -----------------
# df = pd.DataFrame(results)
# df.to_csv("neo4j_base64_benchmark.csv", index=False)
# print("\n✅ Benchmark completed. Results saved to neo4j_base64_benchmark.csv")
# print(df)