from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import os
from tqdm import tqdm
import bson
import logging

# ตั้งค่า logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mongo_transfer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# โหลด environment
load_dotenv()
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client.get_database()

# กำหนด collection
source_col = db["latest28022026"]
target_col = db["androzoo_task_data"]

# สร้าง index
try:
    target_col.create_index("sha256", unique=True)
    logger.info("Index on sha256 created/verified")
except Exception as e:
    logger.warning(f"Index creation issue: {e}")

# โหลด SHA256 ทั้งหมดที่มีอยู่แล้ว
logger.info("Loading existing SHA256 from target collection...")
existing_sha256 = set(doc["sha256"] for doc in target_col.find({}, {"sha256": 1}))
logger.info(f"Found {len(existing_sha256)} existing records")

vt_list = [5, 10, 15, 20, 25, 30, 35]
batch_size = 4000
MAX_BSON_SIZE = 16 * 1024 * 1024

total_inserted = 0
total_skipped_large = 0
total_duplicates = 0

for vt in vt_list:
    logger.info(f"{'='*50}")
    logger.info(f"Processing vt_detection = {vt}")
    logger.info(f"{'='*50}")
    
    # ดึงข้อมูล
    docs = list(source_col.find({
        "vt_detection": vt,
        "$or": [
            {"status": {"$ne": "pending"}},
            {"status": {"$exists": False}}
        ]
    }).limit(batch_size))
    
    logger.info(f"Found {len(docs)} candidate documents")
    
    if not docs:
        logger.info(f"No new docs for VT {vt}")
        continue
    
    # กรอง SHA256 ที่มีอยู่แล้วออก
    new_docs = []
    duplicate_in_batch = set()
    local_sha256_set = set()
    
    for doc in docs:
        sha = doc["sha256"]
        if sha in existing_sha256 or sha in local_sha256_set:
            duplicate_in_batch.add(sha)
            continue
        local_sha256_set.add(sha)
        new_docs.append(doc)
    
    if duplicate_in_batch:
        logger.warning(f"Found {len(duplicate_in_batch)} duplicates before processing")
        total_duplicates += len(duplicate_in_batch)
    
    logger.info(f"After filtering: {len(new_docs)} unique documents")
    
    if not new_docs:
        logger.info(f"No unique documents for VT {vt}")
        continue
    
    # เตรียมข้อมูล
    insert_batch = []
    skipped_large = 0
    
    pbar = tqdm(total=len(new_docs), desc=f"VT {vt}", unit="docs")
    
    for doc in new_docs:
        doc["status"] = doc.get("status", "failed")
        doc["path_file"] = doc.get("path_file", "")
        
        if "report" not in doc:
            doc["report"] = {}
        
        doc["time_update"] = datetime.now()
        
        # ตรวจสอบขนาด BSON
        try:
            encoded = bson.BSON.encode(doc)
            if len(encoded) > MAX_BSON_SIZE:
                skipped_large += 1
                logger.warning(f"Document {doc.get('sha256', 'unknown')} exceeds BSON size")
                pbar.update(1)
                continue
        except Exception as e:
            logger.error(f"BSON encode error: {e}")
            skipped_large += 1
            pbar.update(1)
            continue
        
        insert_batch.append(doc)
        pbar.update(1)
    
    pbar.close()
    
    # Insert
    if insert_batch:
        try:
            result = target_col.insert_many(insert_batch, ordered=False)
            inserted_count = len(result.inserted_ids)
            total_inserted += inserted_count
            
            # อัปเดต existing_sha256 set
            for doc in insert_batch:
                existing_sha256.add(doc["sha256"])
            
            logger.info(f"✓ VT {vt}: Successfully inserted {inserted_count}/{len(insert_batch)} documents")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Batch insert failed: {error_msg[:300]}")
            
            # ถ้ายังมี duplicate ให้ insert ทีละตัว
            if "duplicate key error" in error_msg:
                logger.info(f"Individual insert for remaining {len(insert_batch)} docs...")
                success_count = 0
                dup_count = 0
                
                for idx, doc in enumerate(insert_batch):
                    try:
                        target_col.insert_one(doc)
                        success_count += 1
                        existing_sha256.add(doc["sha256"])
                        total_inserted += 1
                        
                        if (idx + 1) % 500 == 0:
                            logger.info(f"Progress: {idx + 1}/{len(insert_batch)} - {success_count} success, {dup_count} dup")
                            
                    except Exception as single_err:
                        if "duplicate key error" in str(single_err):
                            dup_count += 1
                            total_duplicates += 1
                        else:
                            logger.error(f"Error: {single_err}")
                
                logger.info(f"✓ VT {vt}: Final - {success_count} inserted, {dup_count} duplicates")
            else:
                logger.error(f"Non-duplicate error, skipping VT {vt}")
    else:
        logger.info(f"No documents to insert after BSON check")
    
    total_skipped_large += skipped_large
    logger.info(f"VT {vt} summary: Inserted {total_inserted} total so far")

logger.info(f"{'='*50}")
logger.info(f"FINAL SUMMARY")
logger.info(f"Total inserted: {total_inserted}")
logger.info(f"Total duplicates: {total_duplicates}")
logger.info(f"Total too large: {total_skipped_large}")
logger.info(f"{'='*50}")

client.close()